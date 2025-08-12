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

class PrognosisDataSerializer(serializers.Serializer):
    vehicle_id = serializers.CharField(max_length=50)
    error_code = serializers.CharField(max_length=50)
    datetime = serializers.CharField(max_length=50)
    location_lat = serializers.CharField(max_length=50)
    location_long = serializers.CharField(max_length=50)
    vehicle_location = serializers.CharField(max_length=255)

class PrognosisRequestSerializer(serializers.Serializer):
    data = PrognosisDataSerializer(many=True)


# prognosis/views.py
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.db import transaction, connection
from django.utils.dateparse import parse_datetime
from datetime import datetime
from .models import PrognosisTicket, PrognosisVinDetails, PrognosisTicketErrorcode
from .serializers import PrognosisRequestSerializer
import logging

logger = logging.getLogger(__name__)

class CreatePrognosisTicketView(APIView):
    """
    API endpoint to create prognosis tickets from third-party data
    """
    
    def post(self, request):
        try:
            # Validate incoming data
            serializer = PrognosisRequestSerializer(data=request.data)
            if not serializer.is_valid():
                return Response({
                    'success': False,
                    'message': 'Invalid data format',
                    'errors': serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)
            
            data_list = serializer.validated_data['data']
            
            # Group data by customer_id to create tickets
            customer_groups = {}
            for item in data_list:
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
            logger.error(f"Error creating prognosis tickets: {str(e)}")
            return Response({
                'success': False,
                'message': 'Internal server error',
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def get_customer_id_from_vehicle(self, vehicle_id):
        """
        Map vehicle_id to customer_id from master customer table
        Adjust table name and column names as per your existing schema
        """
        try:
            with connection.cursor() as cursor:
                # Adjust this query based on your actual customer master table structure
                cursor.execute(
                    "SELECT customer_id FROM customer_master WHERE vehicle_id = %s", 
                    [vehicle_id]
                )
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"Error fetching customer_id for vehicle_id {vehicle_id}: {str(e)}")
            return None
    
    def get_error_code_id(self, error_code):
        """
        Map error_code to error_code_id from master error code table
        """
        try:
            with connection.cursor() as cursor:
                # Adjust this query based on your actual error code master table structure
                cursor.execute(
                    "SELECT ID FROM prognosis_errorcode_master WHERE error_code = %s", 
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