# prognosis/models.py
from django.db import models
from django.utils import timezone

class PrognosisTicket(models.Model):
    customer_id = models.BigIntegerField()
    alert_count = models.IntegerField(default=0)
    updated_by = models.IntegerField(null=True, blank=True)
    call_category_id = models.IntegerField(null=True, blank=True)
    call_status_id = models.IntegerField(null=True, blank=True)
    remarks = models.CharField(max_length=250, null=True, blank=True)
    customer_complaint = models.CharField(max_length=250, null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    vehicle_count = models.BigIntegerField(default=0)

    class Meta:
        db_table = 'prognosis_ticket'

class PrognosisVinDetails(models.Model):
    prognosis_ticket = models.ForeignKey(PrognosisTicket, on_delete=models.CASCADE)
    vin_no = models.CharField(max_length=32)
    vehicle_location = models.CharField(max_length=45, null=True, blank=True)
    lat = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    long = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    start_location = models.CharField(max_length=45, null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'prognosis_vin_details'

class PrognosisTicketErrorcode(models.Model):
    error_id = models.BigAutoField(primary_key=True)
    vin = models.ForeignKey(PrognosisVinDetails, on_delete=models.CASCADE)
    ticket = models.ForeignKey(PrognosisTicket, on_delete=models.CASCADE)
    error_type = models.CharField(max_length=255, null=True, blank=True)
    error_desc = models.CharField(max_length=255, null=True, blank=True)
    error_status = models.CharField(max_length=255, default='ACTIVE')
    resolved_time = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    error_code_id = models.BigIntegerField()

    class Meta:
        db_table = 'prognosis_ticket_errorcode'


# prognosis/serializers.py
from rest_framework import serializers
import re

class PrognosisDataSerializer(serializers.Serializer):
    vehicle_id = serializers.CharField(
        max_length=20,
        min_length=1,
        help_text="Alphanumeric vehicle ID, max 20 characters"
    )
    error_code = serializers.CharField(
        max_length=20,
        min_length=1,
        help_text="Error code with alphanumeric and dash/underscore only"
    )
    datetime = serializers.CharField(
        max_length=19,
        help_text="Format: DD.MM.YYYY HH.MM.SS"
    )
    location_lat = serializers.CharField(
        max_length=15,
        allow_blank=True,
        help_text="Latitude coordinate"
    )
    location_long = serializers.CharField(
        max_length=15,
        allow_blank=True,
        help_text="Longitude coordinate"
    )
    vehicle_location = serializers.CharField(
        max_length=255,
        allow_blank=True,
        help_text="Vehicle location description"
    )
    
    def validate_vehicle_id(self, value):
        """Validate vehicle_id format"""
        if not re.match(r'^[a-zA-Z0-9]{1,20}


# prognosis/views.py
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.throttling import AnonRateThrottle, UserRateThrottle
from django.db import transaction
from django.core.exceptions import ValidationError
from django.utils.dateparse import parse_datetime
from datetime import datetime
from decimal import Decimal, InvalidOperation
from .models import PrognosisTicket, PrognosisVinDetails, PrognosisTicketErrorcode
from .serializers import PrognosisRequestSerializer
import logging
import re

logger = logging.getLogger(__name__)

class PrognosisRateThrottle(UserRateThrottle):
    scope = 'prognosis'
    rate = '100/hour'

class CreatePrognosisTicketView(APIView):
    """
    API endpoint to create prognosis tickets from third-party data
    Secured against SQL injection and other vulnerabilities
    """
    
    permission_classes = [IsAuthenticated]  # Require authentication
    throttle_classes = [PrognosisRateThrottle]  # Rate limiting
    
    def post(self, request):
        try:
            # Input size validation
            if len(request.data.get('data', [])) > 1000:  # Limit batch size
                return Response({
                    'success': False,
                    'message': 'Maximum 1000 records allowed per request'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Validate incoming data
            serializer = PrognosisRequestSerializer(data=request.data)
            if not serializer.is_valid():
                return Response({
                    'success': False,
                    'message': 'Invalid data format',
                    'errors': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)
            
            data_list = serializer.validated_data['data']
            
            # Validate and sanitize each record
            validated_data = []
            for item in data_list:
                try:
                    validated_item = self.validate_and_sanitize_record(item)
                    if validated_item:
                        validated_data.append(validated_item)
                except ValidationError as e:
                    logger.warning(f"Validation failed for record: {item}, Error: {str(e)}")
                    continue
            
            if not validated_data:
                return Response({
                    'success': False,
                    'message': 'No valid records found after validation'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Group data by customer_id to create tickets
            customer_groups = {}
            for item in validated_data:
                vehicle_id = item['vehicle_id']
                customer_id = self.get_customer_id_from_vehicle(vehicle_id)
                
                if not customer_id:
                    logger.warning(f"Customer not found for vehicle_id: {vehicle_id}")
                    continue
                
                if customer_id not in customer_groups:
                    customer_groups[customer_id] = []
                customer_groups[customer_id].append(item)
            
            if not customer_groups:
                return Response({
                    'success': False,
                    'message': 'No valid customer data found'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            created_tickets = []
            
            # Process each customer group
            with transaction.atomic():
                for customer_id, customer_data in customer_groups.items():
                    ticket_result = self.create_ticket_for_customer(customer_id, customer_data)
                    if ticket_result:
                        created_tickets.append(ticket_result)
            
            return Response({
                'success': True,
                'message': f'Successfully created {len(created_tickets)} tickets',
                'tickets': created_tickets
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            # Don't expose internal error details in production
            logger.error(f"Error creating prognosis tickets: {str(e)}")
            return Response({
                'success': False,
                'message': 'Internal server error occurred'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def validate_and_sanitize_record(self, item):
        """
        Validate and sanitize each record to prevent injection attacks
        """
        # Vehicle ID validation - alphanumeric only, max 20 chars
        vehicle_id = str(item.get('vehicle_id', '')).strip()
        if not re.match(r'^[a-zA-Z0-9]{1,20}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", vehicle_id):
            raise ValidationError("Invalid vehicle_id format")
        
        # Error code validation - alphanumeric with allowed special chars, max 20 chars
        error_code = str(item.get('error_code', '')).strip().upper()
        if not re.match(r'^[A-Z0-9\-_]{1,20}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", error_code):
            raise ValidationError("Invalid error_code format")
        
        # Datetime validation
        datetime_str = str(item.get('datetime', '')).strip()
        if not re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}\.\d{2}\.\d{2}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", datetime_str):
            raise ValidationError("Invalid datetime format")
        
        # Location validation - numeric values only
        try:
            lat = str(item.get('location_lat', '')).strip()
            long = str(item.get('location_long', '')).strip()
            
            if lat and not re.match(r'^-?\d+\.?\d*
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", lat):
                raise ValidationError("Invalid latitude format")
            if long and not re.match(r'^-?\d+\.?\d*
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", long):
                raise ValidationError("Invalid longitude format")
                
            # Convert and validate decimal ranges
            if lat:
                lat_decimal = Decimal(lat)
                if not (-90 <= lat_decimal <= 90):
                    raise ValidationError("Latitude out of valid range")
                    
            if long:
                long_decimal = Decimal(long)
                if not (-180 <= long_decimal <= 180):
                    raise ValidationError("Longitude out of valid range")
                    
        except (InvalidOperation, ValueError):
            raise ValidationError("Invalid coordinate values")
        
        # Vehicle location validation - limit length and sanitize
        vehicle_location = str(item.get('vehicle_location', '')).strip()
        if len(vehicle_location) > 255:
            vehicle_location = vehicle_location[:255]
        
        # Remove potential SQL injection patterns
        vehicle_location = re.sub(r'[;\'"\\]', '', vehicle_location)
        
        return {
            'vehicle_id': vehicle_id,
            'error_code': error_code,
            'datetime': datetime_str,
            'location_lat': lat,
            'location_long': long,
            'vehicle_location': vehicle_location
        }
    
    def get_customer_id_from_vehicle(self, vehicle_id):
        """
        SECURE: Map vehicle_id to customer_id using Django ORM to prevent SQL injection
        """
        try:
            # Using Django ORM instead of raw SQL for security
            # Adjust this based on your actual Customer model
            from django.db import models
            
            # If you have a Customer model, use it like this:
            # customer = Customer.objects.filter(vehicle_id=vehicle_id).first()
            # return customer.id if customer else None
            
            # For now, using parameterized query as fallback
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT customer_id FROM customer_master WHERE vehicle_id = %s LIMIT 1", 
                    [vehicle_id]
                )
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Error fetching customer_id for vehicle_id {vehicle_id}: {str(e)}")
            return None
    
    def get_error_code_id(self, error_code):
        """
        SECURE: Map error_code to error_code_id using parameterized queries
        """
        try:
            # Using parameterized query to prevent SQL injection
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT ID FROM prognosis_errorcode_master WHERE error_code = %s LIMIT 1", 
                    [error_code]
                )
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Error fetching error_code_id for error_code {error_code}: {str(e)}")
            return None
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", value):
            raise serializers.ValidationError(
                "Vehicle ID must be alphanumeric and max 20 characters"
            )
        return value
    
    def validate_error_code(self, value):
        """Validate error_code format"""
        if not re.match(r'^[A-Z0-9\-_]{1,20}


# prognosis/views.py
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.throttling import AnonRateThrottle, UserRateThrottle
from django.db import transaction
from django.core.exceptions import ValidationError
from django.utils.dateparse import parse_datetime
from datetime import datetime
from decimal import Decimal, InvalidOperation
from .models import PrognosisTicket, PrognosisVinDetails, PrognosisTicketErrorcode
from .serializers import PrognosisRequestSerializer
import logging
import re

logger = logging.getLogger(__name__)

class PrognosisRateThrottle(UserRateThrottle):
    scope = 'prognosis'
    rate = '100/hour'

class CreatePrognosisTicketView(APIView):
    """
    API endpoint to create prognosis tickets from third-party data
    Secured against SQL injection and other vulnerabilities
    """
    
    permission_classes = [IsAuthenticated]  # Require authentication
    throttle_classes = [PrognosisRateThrottle]  # Rate limiting
    
    def post(self, request):
        try:
            # Input size validation
            if len(request.data.get('data', [])) > 1000:  # Limit batch size
                return Response({
                    'success': False,
                    'message': 'Maximum 1000 records allowed per request'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Validate incoming data
            serializer = PrognosisRequestSerializer(data=request.data)
            if not serializer.is_valid():
                return Response({
                    'success': False,
                    'message': 'Invalid data format',
                    'errors': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)
            
            data_list = serializer.validated_data['data']
            
            # Validate and sanitize each record
            validated_data = []
            for item in data_list:
                try:
                    validated_item = self.validate_and_sanitize_record(item)
                    if validated_item:
                        validated_data.append(validated_item)
                except ValidationError as e:
                    logger.warning(f"Validation failed for record: {item}, Error: {str(e)}")
                    continue
            
            if not validated_data:
                return Response({
                    'success': False,
                    'message': 'No valid records found after validation'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Group data by customer_id to create tickets
            customer_groups = {}
            for item in validated_data:
                vehicle_id = item['vehicle_id']
                customer_id = self.get_customer_id_from_vehicle(vehicle_id)
                
                if not customer_id:
                    logger.warning(f"Customer not found for vehicle_id: {vehicle_id}")
                    continue
                
                if customer_id not in customer_groups:
                    customer_groups[customer_id] = []
                customer_groups[customer_id].append(item)
            
            if not customer_groups:
                return Response({
                    'success': False,
                    'message': 'No valid customer data found'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            created_tickets = []
            
            # Process each customer group
            with transaction.atomic():
                for customer_id, customer_data in customer_groups.items():
                    ticket_result = self.create_ticket_for_customer(customer_id, customer_data)
                    if ticket_result:
                        created_tickets.append(ticket_result)
            
            return Response({
                'success': True,
                'message': f'Successfully created {len(created_tickets)} tickets',
                'tickets': created_tickets
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            # Don't expose internal error details in production
            logger.error(f"Error creating prognosis tickets: {str(e)}")
            return Response({
                'success': False,
                'message': 'Internal server error occurred'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def validate_and_sanitize_record(self, item):
        """
        Validate and sanitize each record to prevent injection attacks
        """
        # Vehicle ID validation - alphanumeric only, max 20 chars
        vehicle_id = str(item.get('vehicle_id', '')).strip()
        if not re.match(r'^[a-zA-Z0-9]{1,20}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", vehicle_id):
            raise ValidationError("Invalid vehicle_id format")
        
        # Error code validation - alphanumeric with allowed special chars, max 20 chars
        error_code = str(item.get('error_code', '')).strip().upper()
        if not re.match(r'^[A-Z0-9\-_]{1,20}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", error_code):
            raise ValidationError("Invalid error_code format")
        
        # Datetime validation
        datetime_str = str(item.get('datetime', '')).strip()
        if not re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}\.\d{2}\.\d{2}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", datetime_str):
            raise ValidationError("Invalid datetime format")
        
        # Location validation - numeric values only
        try:
            lat = str(item.get('location_lat', '')).strip()
            long = str(item.get('location_long', '')).strip()
            
            if lat and not re.match(r'^-?\d+\.?\d*
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", lat):
                raise ValidationError("Invalid latitude format")
            if long and not re.match(r'^-?\d+\.?\d*
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", long):
                raise ValidationError("Invalid longitude format")
                
            # Convert and validate decimal ranges
            if lat:
                lat_decimal = Decimal(lat)
                if not (-90 <= lat_decimal <= 90):
                    raise ValidationError("Latitude out of valid range")
                    
            if long:
                long_decimal = Decimal(long)
                if not (-180 <= long_decimal <= 180):
                    raise ValidationError("Longitude out of valid range")
                    
        except (InvalidOperation, ValueError):
            raise ValidationError("Invalid coordinate values")
        
        # Vehicle location validation - limit length and sanitize
        vehicle_location = str(item.get('vehicle_location', '')).strip()
        if len(vehicle_location) > 255:
            vehicle_location = vehicle_location[:255]
        
        # Remove potential SQL injection patterns
        vehicle_location = re.sub(r'[;\'"\\]', '', vehicle_location)
        
        return {
            'vehicle_id': vehicle_id,
            'error_code': error_code,
            'datetime': datetime_str,
            'location_lat': lat,
            'location_long': long,
            'vehicle_location': vehicle_location
        }
    
    def get_customer_id_from_vehicle(self, vehicle_id):
        """
        SECURE: Map vehicle_id to customer_id using Django ORM to prevent SQL injection
        """
        try:
            # Using Django ORM instead of raw SQL for security
            # Adjust this based on your actual Customer model
            from django.db import models
            
            # If you have a Customer model, use it like this:
            # customer = Customer.objects.filter(vehicle_id=vehicle_id).first()
            # return customer.id if customer else None
            
            # For now, using parameterized query as fallback
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT customer_id FROM customer_master WHERE vehicle_id = %s LIMIT 1", 
                    [vehicle_id]
                )
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Error fetching customer_id for vehicle_id {vehicle_id}: {str(e)}")
            return None
    
    def get_error_code_id(self, error_code):
        """
        SECURE: Map error_code to error_code_id using parameterized queries
        """
        try:
            # Using parameterized query to prevent SQL injection
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT ID FROM prognosis_errorcode_master WHERE error_code = %s LIMIT 1", 
                    [error_code]
                )
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Error fetching error_code_id for error_code {error_code}: {str(e)}")
            return None
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", value.upper()):
            raise serializers.ValidationError(
                "Error code must be alphanumeric with dash/underscore only, max 20 characters"
            )
        return value.upper()
    
    def validate_datetime(self, value):
        """Validate datetime format"""
        if not re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}\.\d{2}\.\d{2}


# prognosis/views.py
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.throttling import AnonRateThrottle, UserRateThrottle
from django.db import transaction
from django.core.exceptions import ValidationError
from django.utils.dateparse import parse_datetime
from datetime import datetime
from decimal import Decimal, InvalidOperation
from .models import PrognosisTicket, PrognosisVinDetails, PrognosisTicketErrorcode
from .serializers import PrognosisRequestSerializer
import logging
import re

logger = logging.getLogger(__name__)

class PrognosisRateThrottle(UserRateThrottle):
    scope = 'prognosis'
    rate = '100/hour'

class CreatePrognosisTicketView(APIView):
    """
    API endpoint to create prognosis tickets from third-party data
    Secured against SQL injection and other vulnerabilities
    """
    
    permission_classes = [IsAuthenticated]  # Require authentication
    throttle_classes = [PrognosisRateThrottle]  # Rate limiting
    
    def post(self, request):
        try:
            # Input size validation
            if len(request.data.get('data', [])) > 1000:  # Limit batch size
                return Response({
                    'success': False,
                    'message': 'Maximum 1000 records allowed per request'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Validate incoming data
            serializer = PrognosisRequestSerializer(data=request.data)
            if not serializer.is_valid():
                return Response({
                    'success': False,
                    'message': 'Invalid data format',
                    'errors': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)
            
            data_list = serializer.validated_data['data']
            
            # Validate and sanitize each record
            validated_data = []
            for item in data_list:
                try:
                    validated_item = self.validate_and_sanitize_record(item)
                    if validated_item:
                        validated_data.append(validated_item)
                except ValidationError as e:
                    logger.warning(f"Validation failed for record: {item}, Error: {str(e)}")
                    continue
            
            if not validated_data:
                return Response({
                    'success': False,
                    'message': 'No valid records found after validation'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Group data by customer_id to create tickets
            customer_groups = {}
            for item in validated_data:
                vehicle_id = item['vehicle_id']
                customer_id = self.get_customer_id_from_vehicle(vehicle_id)
                
                if not customer_id:
                    logger.warning(f"Customer not found for vehicle_id: {vehicle_id}")
                    continue
                
                if customer_id not in customer_groups:
                    customer_groups[customer_id] = []
                customer_groups[customer_id].append(item)
            
            if not customer_groups:
                return Response({
                    'success': False,
                    'message': 'No valid customer data found'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            created_tickets = []
            
            # Process each customer group
            with transaction.atomic():
                for customer_id, customer_data in customer_groups.items():
                    ticket_result = self.create_ticket_for_customer(customer_id, customer_data)
                    if ticket_result:
                        created_tickets.append(ticket_result)
            
            return Response({
                'success': True,
                'message': f'Successfully created {len(created_tickets)} tickets',
                'tickets': created_tickets
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            # Don't expose internal error details in production
            logger.error(f"Error creating prognosis tickets: {str(e)}")
            return Response({
                'success': False,
                'message': 'Internal server error occurred'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def validate_and_sanitize_record(self, item):
        """
        Validate and sanitize each record to prevent injection attacks
        """
        # Vehicle ID validation - alphanumeric only, max 20 chars
        vehicle_id = str(item.get('vehicle_id', '')).strip()
        if not re.match(r'^[a-zA-Z0-9]{1,20}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", vehicle_id):
            raise ValidationError("Invalid vehicle_id format")
        
        # Error code validation - alphanumeric with allowed special chars, max 20 chars
        error_code = str(item.get('error_code', '')).strip().upper()
        if not re.match(r'^[A-Z0-9\-_]{1,20}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", error_code):
            raise ValidationError("Invalid error_code format")
        
        # Datetime validation
        datetime_str = str(item.get('datetime', '')).strip()
        if not re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}\.\d{2}\.\d{2}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", datetime_str):
            raise ValidationError("Invalid datetime format")
        
        # Location validation - numeric values only
        try:
            lat = str(item.get('location_lat', '')).strip()
            long = str(item.get('location_long', '')).strip()
            
            if lat and not re.match(r'^-?\d+\.?\d*
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", lat):
                raise ValidationError("Invalid latitude format")
            if long and not re.match(r'^-?\d+\.?\d*
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", long):
                raise ValidationError("Invalid longitude format")
                
            # Convert and validate decimal ranges
            if lat:
                lat_decimal = Decimal(lat)
                if not (-90 <= lat_decimal <= 90):
                    raise ValidationError("Latitude out of valid range")
                    
            if long:
                long_decimal = Decimal(long)
                if not (-180 <= long_decimal <= 180):
                    raise ValidationError("Longitude out of valid range")
                    
        except (InvalidOperation, ValueError):
            raise ValidationError("Invalid coordinate values")
        
        # Vehicle location validation - limit length and sanitize
        vehicle_location = str(item.get('vehicle_location', '')).strip()
        if len(vehicle_location) > 255:
            vehicle_location = vehicle_location[:255]
        
        # Remove potential SQL injection patterns
        vehicle_location = re.sub(r'[;\'"\\]', '', vehicle_location)
        
        return {
            'vehicle_id': vehicle_id,
            'error_code': error_code,
            'datetime': datetime_str,
            'location_lat': lat,
            'location_long': long,
            'vehicle_location': vehicle_location
        }
    
    def get_customer_id_from_vehicle(self, vehicle_id):
        """
        SECURE: Map vehicle_id to customer_id using Django ORM to prevent SQL injection
        """
        try:
            # Using Django ORM instead of raw SQL for security
            # Adjust this based on your actual Customer model
            from django.db import models
            
            # If you have a Customer model, use it like this:
            # customer = Customer.objects.filter(vehicle_id=vehicle_id).first()
            # return customer.id if customer else None
            
            # For now, using parameterized query as fallback
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT customer_id FROM customer_master WHERE vehicle_id = %s LIMIT 1", 
                    [vehicle_id]
                )
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Error fetching customer_id for vehicle_id {vehicle_id}: {str(e)}")
            return None
    
    def get_error_code_id(self, error_code):
        """
        SECURE: Map error_code to error_code_id using parameterized queries
        """
        try:
            # Using parameterized query to prevent SQL injection
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT ID FROM prognosis_errorcode_master WHERE error_code = %s LIMIT 1", 
                    [error_code]
                )
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Error fetching error_code_id for error_code {error_code}: {str(e)}")
            return None
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", value):
            raise serializers.ValidationError(
                "Datetime must be in format DD.MM.YYYY HH.MM.SS"
            )
        return value
    
    def validate_location_lat(self, value):
        """Validate latitude"""
        if value and not re.match(r'^-?\d+\.?\d*


# prognosis/views.py
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.throttling import AnonRateThrottle, UserRateThrottle
from django.db import transaction
from django.core.exceptions import ValidationError
from django.utils.dateparse import parse_datetime
from datetime import datetime
from decimal import Decimal, InvalidOperation
from .models import PrognosisTicket, PrognosisVinDetails, PrognosisTicketErrorcode
from .serializers import PrognosisRequestSerializer
import logging
import re

logger = logging.getLogger(__name__)

class PrognosisRateThrottle(UserRateThrottle):
    scope = 'prognosis'
    rate = '100/hour'

class CreatePrognosisTicketView(APIView):
    """
    API endpoint to create prognosis tickets from third-party data
    Secured against SQL injection and other vulnerabilities
    """
    
    permission_classes = [IsAuthenticated]  # Require authentication
    throttle_classes = [PrognosisRateThrottle]  # Rate limiting
    
    def post(self, request):
        try:
            # Input size validation
            if len(request.data.get('data', [])) > 1000:  # Limit batch size
                return Response({
                    'success': False,
                    'message': 'Maximum 1000 records allowed per request'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Validate incoming data
            serializer = PrognosisRequestSerializer(data=request.data)
            if not serializer.is_valid():
                return Response({
                    'success': False,
                    'message': 'Invalid data format',
                    'errors': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)
            
            data_list = serializer.validated_data['data']
            
            # Validate and sanitize each record
            validated_data = []
            for item in data_list:
                try:
                    validated_item = self.validate_and_sanitize_record(item)
                    if validated_item:
                        validated_data.append(validated_item)
                except ValidationError as e:
                    logger.warning(f"Validation failed for record: {item}, Error: {str(e)}")
                    continue
            
            if not validated_data:
                return Response({
                    'success': False,
                    'message': 'No valid records found after validation'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Group data by customer_id to create tickets
            customer_groups = {}
            for item in validated_data:
                vehicle_id = item['vehicle_id']
                customer_id = self.get_customer_id_from_vehicle(vehicle_id)
                
                if not customer_id:
                    logger.warning(f"Customer not found for vehicle_id: {vehicle_id}")
                    continue
                
                if customer_id not in customer_groups:
                    customer_groups[customer_id] = []
                customer_groups[customer_id].append(item)
            
            if not customer_groups:
                return Response({
                    'success': False,
                    'message': 'No valid customer data found'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            created_tickets = []
            
            # Process each customer group
            with transaction.atomic():
                for customer_id, customer_data in customer_groups.items():
                    ticket_result = self.create_ticket_for_customer(customer_id, customer_data)
                    if ticket_result:
                        created_tickets.append(ticket_result)
            
            return Response({
                'success': True,
                'message': f'Successfully created {len(created_tickets)} tickets',
                'tickets': created_tickets
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            # Don't expose internal error details in production
            logger.error(f"Error creating prognosis tickets: {str(e)}")
            return Response({
                'success': False,
                'message': 'Internal server error occurred'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def validate_and_sanitize_record(self, item):
        """
        Validate and sanitize each record to prevent injection attacks
        """
        # Vehicle ID validation - alphanumeric only, max 20 chars
        vehicle_id = str(item.get('vehicle_id', '')).strip()
        if not re.match(r'^[a-zA-Z0-9]{1,20}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", vehicle_id):
            raise ValidationError("Invalid vehicle_id format")
        
        # Error code validation - alphanumeric with allowed special chars, max 20 chars
        error_code = str(item.get('error_code', '')).strip().upper()
        if not re.match(r'^[A-Z0-9\-_]{1,20}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", error_code):
            raise ValidationError("Invalid error_code format")
        
        # Datetime validation
        datetime_str = str(item.get('datetime', '')).strip()
        if not re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}\.\d{2}\.\d{2}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", datetime_str):
            raise ValidationError("Invalid datetime format")
        
        # Location validation - numeric values only
        try:
            lat = str(item.get('location_lat', '')).strip()
            long = str(item.get('location_long', '')).strip()
            
            if lat and not re.match(r'^-?\d+\.?\d*
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", lat):
                raise ValidationError("Invalid latitude format")
            if long and not re.match(r'^-?\d+\.?\d*
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", long):
                raise ValidationError("Invalid longitude format")
                
            # Convert and validate decimal ranges
            if lat:
                lat_decimal = Decimal(lat)
                if not (-90 <= lat_decimal <= 90):
                    raise ValidationError("Latitude out of valid range")
                    
            if long:
                long_decimal = Decimal(long)
                if not (-180 <= long_decimal <= 180):
                    raise ValidationError("Longitude out of valid range")
                    
        except (InvalidOperation, ValueError):
            raise ValidationError("Invalid coordinate values")
        
        # Vehicle location validation - limit length and sanitize
        vehicle_location = str(item.get('vehicle_location', '')).strip()
        if len(vehicle_location) > 255:
            vehicle_location = vehicle_location[:255]
        
        # Remove potential SQL injection patterns
        vehicle_location = re.sub(r'[;\'"\\]', '', vehicle_location)
        
        return {
            'vehicle_id': vehicle_id,
            'error_code': error_code,
            'datetime': datetime_str,
            'location_lat': lat,
            'location_long': long,
            'vehicle_location': vehicle_location
        }
    
    def get_customer_id_from_vehicle(self, vehicle_id):
        """
        SECURE: Map vehicle_id to customer_id using Django ORM to prevent SQL injection
        """
        try:
            # Using Django ORM instead of raw SQL for security
            # Adjust this based on your actual Customer model
            from django.db import models
            
            # If you have a Customer model, use it like this:
            # customer = Customer.objects.filter(vehicle_id=vehicle_id).first()
            # return customer.id if customer else None
            
            # For now, using parameterized query as fallback
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT customer_id FROM customer_master WHERE vehicle_id = %s LIMIT 1", 
                    [vehicle_id]
                )
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Error fetching customer_id for vehicle_id {vehicle_id}: {str(e)}")
            return None
    
    def get_error_code_id(self, error_code):
        """
        SECURE: Map error_code to error_code_id using parameterized queries
        """
        try:
            # Using parameterized query to prevent SQL injection
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT ID FROM prognosis_errorcode_master WHERE error_code = %s LIMIT 1", 
                    [error_code]
                )
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Error fetching error_code_id for error_code {error_code}: {str(e)}")
            return None
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", value):
            raise serializers.ValidationError("Invalid latitude format")
        return value
    
    def validate_location_long(self, value):
        """Validate longitude"""
        if value and not re.match(r'^-?\d+\.?\d*


# prognosis/views.py
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.throttling import AnonRateThrottle, UserRateThrottle
from django.db import transaction
from django.core.exceptions import ValidationError
from django.utils.dateparse import parse_datetime
from datetime import datetime
from decimal import Decimal, InvalidOperation
from .models import PrognosisTicket, PrognosisVinDetails, PrognosisTicketErrorcode
from .serializers import PrognosisRequestSerializer
import logging
import re

logger = logging.getLogger(__name__)

class PrognosisRateThrottle(UserRateThrottle):
    scope = 'prognosis'
    rate = '100/hour'

class CreatePrognosisTicketView(APIView):
    """
    API endpoint to create prognosis tickets from third-party data
    Secured against SQL injection and other vulnerabilities
    """
    
    permission_classes = [IsAuthenticated]  # Require authentication
    throttle_classes = [PrognosisRateThrottle]  # Rate limiting
    
    def post(self, request):
        try:
            # Input size validation
            if len(request.data.get('data', [])) > 1000:  # Limit batch size
                return Response({
                    'success': False,
                    'message': 'Maximum 1000 records allowed per request'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Validate incoming data
            serializer = PrognosisRequestSerializer(data=request.data)
            if not serializer.is_valid():
                return Response({
                    'success': False,
                    'message': 'Invalid data format',
                    'errors': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)
            
            data_list = serializer.validated_data['data']
            
            # Validate and sanitize each record
            validated_data = []
            for item in data_list:
                try:
                    validated_item = self.validate_and_sanitize_record(item)
                    if validated_item:
                        validated_data.append(validated_item)
                except ValidationError as e:
                    logger.warning(f"Validation failed for record: {item}, Error: {str(e)}")
                    continue
            
            if not validated_data:
                return Response({
                    'success': False,
                    'message': 'No valid records found after validation'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Group data by customer_id to create tickets
            customer_groups = {}
            for item in validated_data:
                vehicle_id = item['vehicle_id']
                customer_id = self.get_customer_id_from_vehicle(vehicle_id)
                
                if not customer_id:
                    logger.warning(f"Customer not found for vehicle_id: {vehicle_id}")
                    continue
                
                if customer_id not in customer_groups:
                    customer_groups[customer_id] = []
                customer_groups[customer_id].append(item)
            
            if not customer_groups:
                return Response({
                    'success': False,
                    'message': 'No valid customer data found'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            created_tickets = []
            
            # Process each customer group
            with transaction.atomic():
                for customer_id, customer_data in customer_groups.items():
                    ticket_result = self.create_ticket_for_customer(customer_id, customer_data)
                    if ticket_result:
                        created_tickets.append(ticket_result)
            
            return Response({
                'success': True,
                'message': f'Successfully created {len(created_tickets)} tickets',
                'tickets': created_tickets
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            # Don't expose internal error details in production
            logger.error(f"Error creating prognosis tickets: {str(e)}")
            return Response({
                'success': False,
                'message': 'Internal server error occurred'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def validate_and_sanitize_record(self, item):
        """
        Validate and sanitize each record to prevent injection attacks
        """
        # Vehicle ID validation - alphanumeric only, max 20 chars
        vehicle_id = str(item.get('vehicle_id', '')).strip()
        if not re.match(r'^[a-zA-Z0-9]{1,20}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", vehicle_id):
            raise ValidationError("Invalid vehicle_id format")
        
        # Error code validation - alphanumeric with allowed special chars, max 20 chars
        error_code = str(item.get('error_code', '')).strip().upper()
        if not re.match(r'^[A-Z0-9\-_]{1,20}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", error_code):
            raise ValidationError("Invalid error_code format")
        
        # Datetime validation
        datetime_str = str(item.get('datetime', '')).strip()
        if not re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}\.\d{2}\.\d{2}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", datetime_str):
            raise ValidationError("Invalid datetime format")
        
        # Location validation - numeric values only
        try:
            lat = str(item.get('location_lat', '')).strip()
            long = str(item.get('location_long', '')).strip()
            
            if lat and not re.match(r'^-?\d+\.?\d*
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", lat):
                raise ValidationError("Invalid latitude format")
            if long and not re.match(r'^-?\d+\.?\d*
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", long):
                raise ValidationError("Invalid longitude format")
                
            # Convert and validate decimal ranges
            if lat:
                lat_decimal = Decimal(lat)
                if not (-90 <= lat_decimal <= 90):
                    raise ValidationError("Latitude out of valid range")
                    
            if long:
                long_decimal = Decimal(long)
                if not (-180 <= long_decimal <= 180):
                    raise ValidationError("Longitude out of valid range")
                    
        except (InvalidOperation, ValueError):
            raise ValidationError("Invalid coordinate values")
        
        # Vehicle location validation - limit length and sanitize
        vehicle_location = str(item.get('vehicle_location', '')).strip()
        if len(vehicle_location) > 255:
            vehicle_location = vehicle_location[:255]
        
        # Remove potential SQL injection patterns
        vehicle_location = re.sub(r'[;\'"\\]', '', vehicle_location)
        
        return {
            'vehicle_id': vehicle_id,
            'error_code': error_code,
            'datetime': datetime_str,
            'location_lat': lat,
            'location_long': long,
            'vehicle_location': vehicle_location
        }
    
    def get_customer_id_from_vehicle(self, vehicle_id):
        """
        SECURE: Map vehicle_id to customer_id using Django ORM to prevent SQL injection
        """
        try:
            # Using Django ORM instead of raw SQL for security
            # Adjust this based on your actual Customer model
            from django.db import models
            
            # If you have a Customer model, use it like this:
            # customer = Customer.objects.filter(vehicle_id=vehicle_id).first()
            # return customer.id if customer else None
            
            # For now, using parameterized query as fallback
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT customer_id FROM customer_master WHERE vehicle_id = %s LIMIT 1", 
                    [vehicle_id]
                )
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Error fetching customer_id for vehicle_id {vehicle_id}: {str(e)}")
            return None
    
    def get_error_code_id(self, error_code):
        """
        SECURE: Map error_code to error_code_id using parameterized queries
        """
        try:
            # Using parameterized query to prevent SQL injection
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT ID FROM prognosis_errorcode_master WHERE error_code = %s LIMIT 1", 
                    [error_code]
                )
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Error fetching error_code_id for error_code {error_code}: {str(e)}")
            return None
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", value):
            raise serializers.ValidationError("Invalid longitude format")
        return value

class PrognosisRequestSerializer(serializers.Serializer):
    data = PrognosisDataSerializer(many=True)
    
    def validate_data(self, value):
        """Validate data array"""
        if not value:
            raise serializers.ValidationError("Data array cannot be empty")
        
        if len(value) > 1000:
            raise serializers.ValidationError("Maximum 1000 records allowed per request")
        
        return value


# prognosis/views.py
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.throttling import AnonRateThrottle, UserRateThrottle
from django.db import transaction
from django.core.exceptions import ValidationError
from django.utils.dateparse import parse_datetime
from datetime import datetime
from decimal import Decimal, InvalidOperation
from .models import PrognosisTicket, PrognosisVinDetails, PrognosisTicketErrorcode
from .serializers import PrognosisRequestSerializer
import logging
import re

logger = logging.getLogger(__name__)

class PrognosisRateThrottle(UserRateThrottle):
    scope = 'prognosis'
    rate = '100/hour'

class CreatePrognosisTicketView(APIView):
    """
    API endpoint to create prognosis tickets from third-party data
    Secured against SQL injection and other vulnerabilities
    """
    
    permission_classes = [IsAuthenticated]  # Require authentication
    throttle_classes = [PrognosisRateThrottle]  # Rate limiting
    
    def post(self, request):
        try:
            # Input size validation
            if len(request.data.get('data', [])) > 1000:  # Limit batch size
                return Response({
                    'success': False,
                    'message': 'Maximum 1000 records allowed per request'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Validate incoming data
            serializer = PrognosisRequestSerializer(data=request.data)
            if not serializer.is_valid():
                return Response({
                    'success': False,
                    'message': 'Invalid data format',
                    'errors': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)
            
            data_list = serializer.validated_data['data']
            
            # Validate and sanitize each record
            validated_data = []
            for item in data_list:
                try:
                    validated_item = self.validate_and_sanitize_record(item)
                    if validated_item:
                        validated_data.append(validated_item)
                except ValidationError as e:
                    logger.warning(f"Validation failed for record: {item}, Error: {str(e)}")
                    continue
            
            if not validated_data:
                return Response({
                    'success': False,
                    'message': 'No valid records found after validation'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Group data by customer_id to create tickets
            customer_groups = {}
            for item in validated_data:
                vehicle_id = item['vehicle_id']
                customer_id = self.get_customer_id_from_vehicle(vehicle_id)
                
                if not customer_id:
                    logger.warning(f"Customer not found for vehicle_id: {vehicle_id}")
                    continue
                
                if customer_id not in customer_groups:
                    customer_groups[customer_id] = []
                customer_groups[customer_id].append(item)
            
            if not customer_groups:
                return Response({
                    'success': False,
                    'message': 'No valid customer data found'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            created_tickets = []
            
            # Process each customer group
            with transaction.atomic():
                for customer_id, customer_data in customer_groups.items():
                    ticket_result = self.create_ticket_for_customer(customer_id, customer_data)
                    if ticket_result:
                        created_tickets.append(ticket_result)
            
            return Response({
                'success': True,
                'message': f'Successfully created {len(created_tickets)} tickets',
                'tickets': created_tickets
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            # Don't expose internal error details in production
            logger.error(f"Error creating prognosis tickets: {str(e)}")
            return Response({
                'success': False,
                'message': 'Internal server error occurred'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def validate_and_sanitize_record(self, item):
        """
        Validate and sanitize each record to prevent injection attacks
        """
        # Vehicle ID validation - alphanumeric only, max 20 chars
        vehicle_id = str(item.get('vehicle_id', '')).strip()
        if not re.match(r'^[a-zA-Z0-9]{1,20}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", vehicle_id):
            raise ValidationError("Invalid vehicle_id format")
        
        # Error code validation - alphanumeric with allowed special chars, max 20 chars
        error_code = str(item.get('error_code', '')).strip().upper()
        if not re.match(r'^[A-Z0-9\-_]{1,20}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", error_code):
            raise ValidationError("Invalid error_code format")
        
        # Datetime validation
        datetime_str = str(item.get('datetime', '')).strip()
        if not re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}\.\d{2}\.\d{2}
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", datetime_str):
            raise ValidationError("Invalid datetime format")
        
        # Location validation - numeric values only
        try:
            lat = str(item.get('location_lat', '')).strip()
            long = str(item.get('location_long', '')).strip()
            
            if lat and not re.match(r'^-?\d+\.?\d*
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", lat):
                raise ValidationError("Invalid latitude format")
            if long and not re.match(r'^-?\d+\.?\d*
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
""", long):
                raise ValidationError("Invalid longitude format")
                
            # Convert and validate decimal ranges
            if lat:
                lat_decimal = Decimal(lat)
                if not (-90 <= lat_decimal <= 90):
                    raise ValidationError("Latitude out of valid range")
                    
            if long:
                long_decimal = Decimal(long)
                if not (-180 <= long_decimal <= 180):
                    raise ValidationError("Longitude out of valid range")
                    
        except (InvalidOperation, ValueError):
            raise ValidationError("Invalid coordinate values")
        
        # Vehicle location validation - limit length and sanitize
        vehicle_location = str(item.get('vehicle_location', '')).strip()
        if len(vehicle_location) > 255:
            vehicle_location = vehicle_location[:255]
        
        # Remove potential SQL injection patterns
        vehicle_location = re.sub(r'[;\'"\\]', '', vehicle_location)
        
        return {
            'vehicle_id': vehicle_id,
            'error_code': error_code,
            'datetime': datetime_str,
            'location_lat': lat,
            'location_long': long,
            'vehicle_location': vehicle_location
        }
    
    def get_customer_id_from_vehicle(self, vehicle_id):
        """
        SECURE: Map vehicle_id to customer_id using Django ORM to prevent SQL injection
        """
        try:
            # Using Django ORM instead of raw SQL for security
            # Adjust this based on your actual Customer model
            from django.db import models
            
            # If you have a Customer model, use it like this:
            # customer = Customer.objects.filter(vehicle_id=vehicle_id).first()
            # return customer.id if customer else None
            
            # For now, using parameterized query as fallback
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT customer_id FROM customer_master WHERE vehicle_id = %s LIMIT 1", 
                    [vehicle_id]
                )
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Error fetching customer_id for vehicle_id {vehicle_id}: {str(e)}")
            return None
    
    def get_error_code_id(self, error_code):
        """
        SECURE: Map error_code to error_code_id using parameterized queries
        """
        try:
            # Using parameterized query to prevent SQL injection
            from django.db import connection
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT ID FROM prognosis_errorcode_master WHERE error_code = %s LIMIT 1", 
                    [error_code]
                )
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logger.error(f"Error fetching error_code_id for error_code {error_code}: {str(e)}")
            return None
    
    def parse_datetime_string(self, datetime_str):
        """
        Parse datetime string from format: "12.08.2025 11.10.00"
        """
        try:
            return datetime.strptime(datetime_str, "%d.%m.%Y %H.%M.%S")
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_str}")
            return datetime.now()
    
    def create_ticket_for_customer(self, customer_id, customer_data):
        """
        Create a ticket and related records for a specific customer
        """
        try:
            # Group by unique vehicles for this customer
            vehicle_groups = {}
            total_alerts = 0
            
            for item in customer_data:
                vehicle_id = item['vehicle_id']
                if vehicle_id not in vehicle_groups:
                    vehicle_groups[vehicle_id] = []
                vehicle_groups[vehicle_id].append(item)
                total_alerts += 1
            
            # Create the main ticket
            ticket = PrognosisTicket.objects.create(
                customer_id=customer_id,
                alert_count=total_alerts,
                vehicle_count=len(vehicle_groups),
                call_status_id=1,  # Default to open status
                remarks=f"Auto-created ticket for {len(vehicle_groups)} vehicles with {total_alerts} alerts"
            )
            
            # Process each vehicle
            for vehicle_id, vehicle_data in vehicle_groups.items():
                # Create VIN details record
                first_record = vehicle_data[0]  # Use first record for location data
                
                vin_detail = PrognosisVinDetails.objects.create(
                    prognosis_ticket=ticket,
                    vin_no=vehicle_id,  # Using vehicle_id as VIN for now
                    vehicle_location=first_record['vehicle_location'],
                    lat=self.safe_decimal(first_record['location_lat']),
                    long=self.safe_decimal(first_record['location_long'])
                )
                
                # Create error code records for each error in this vehicle
                for record in vehicle_data:
                    error_code_id = self.get_error_code_id(record['error_code'])
                    
                    if error_code_id:
                        PrognosisTicketErrorcode.objects.create(
                            vin=vin_detail,
                            ticket=ticket,
                            error_code_id=error_code_id,
                            error_type=record['error_code'],
                            error_desc=f"Error {record['error_code']} detected",
                            error_status='ACTIVE'
                        )
                    else:
                        logger.warning(f"Error code not found in master table: {record['error_code']}")
            
            return {
                'ticket_id': ticket.id,
                'customer_id': customer_id,
                'vehicle_count': len(vehicle_groups),
                'alert_count': total_alerts
            }
            
        except Exception as e:
            logger.error(f"Error creating ticket for customer {customer_id}: {str(e)}")
            raise
    
    def safe_decimal(self, value):
        """
        Safely convert string to decimal, handling potential conversion errors
        """
        try:
            return float(value) if value else None
        except (ValueError, TypeError):
            return None


# prognosis/urls.py
from django.urls import path
from .views import CreatePrognosisTicketView

urlpatterns = [
    path('create-ticket/', CreatePrognosisTicketView.as_view(), name='create_prognosis_ticket'),
]


# prognosis/apps.py
from django.apps import AppConfig

class PrognosisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'prognosis'


# Add this to your main urls.py
"""
from django.urls import path, include

urlpatterns = [
    # ... your existing patterns
    path('api/prognosis/', include('prognosis.urls')),
]
"""

# Add this to your settings.py INSTALLED_APPS
"""
INSTALLED_APPS = [
    # ... your existing apps
    'prognosis',
    'rest_framework',
]
"""
