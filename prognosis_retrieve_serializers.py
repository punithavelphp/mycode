# prognosis/retrieve_serializers.py
from rest_framework import serializers
from .models import PrognosisTicket, PrognosisVinDetails, PrognosisTicketErrorcode
from datetime import datetime, date
import re

class VehicleDetailSerializer(serializers.ModelSerializer):
    """Serializer for vehicle details in ticket responses"""
    
    class Meta:
        model = PrognosisVinDetails
        fields = [
            'id', 'vin_no', 'vehicle_location', 'lat', 'long', 
            'start_location', 'created_at', 'updated_at'
        ]
    
    def to_representation(self, instance):
        data = super().to_representation(instance)
        # Convert Decimal to float for JSON serialization
        if data['lat']:
            data['lat'] = float(data['lat'])
        if data['long']:
            data['long'] = float(data['long'])
        return data

class ErrorCodeDetailSerializer(serializers.ModelSerializer):
    """Serializer for error code details in ticket responses"""
    
    error_code_info = serializers.SerializerMethodField()
    
    class Meta:
        model = PrognosisTicketErrorcode
        fields = [
            'error_id', 'error_type', 'error_desc', 'error_status',
            'resolved_time', 'created_at', 'updated_at', 'error_code_info'
        ]
    
    def get_error_code_info(self, obj):
        """Get additional error code information from master table"""
        try:
            # You can enhance this to fetch from error code master table
            return {
                'error_code_id': obj.error_code_id,
                'severity': 'HIGH' if obj.error_type and obj.error_type.startswith('P') else 'MEDIUM',
                'category': self.get_error_category(obj.error_type)
            }
        except Exception:
            return None
    
    def get_error_category(self, error_type):
        """Categorize error types"""
        if not error_type:
            return 'UNKNOWN'
        
        error_type = error_type.upper()
        if error_type.startswith('P'):
            return 'POWERTRAIN'
        elif error_type.startswith('B'):
            return 'BODY'
        elif error_type.startswith('C'):
            return 'CHASSIS'
        elif error_type.startswith('U'):
            return 'NETWORK'
        else:
            return 'OTHER'

class TicketListSerializer(serializers.ModelSerializer):
    """Serializer for ticket list view with summary information"""
    
    vehicle_count_actual = serializers.IntegerField(read_only=True)
    error_count = serializers.IntegerField(read_only=True)
    status_display = serializers.SerializerMethodField()
    vehicles_summary = serializers.SerializerMethodField()
    errors_summary = serializers.SerializerMethodField()
    
    class Meta:
        model = PrognosisTicket
        fields = [
            'id', 'customer_id', 'alert_count', 'vehicle_count', 
            'vehicle_count_actual', 'error_count', 'call_status_id',
            'status_display', 'remarks', 'customer_complaint',
            'created_at', 'updated_at', 'vehicles_summary', 'errors_summary'
        ]
    
    def get_status_display(self, obj):
        """Get human-readable status"""
        status_map = {
            1: 'Open',
            2: 'In Progress',
            3: 'Resolved',
            4: 'Closed',
            5: 'Cancelled'
        }
        return status_map.get(obj.call_status_id, 'Unknown')
    
    def get_vehicles_summary(self, obj):
        """Get summary of vehicles in this ticket"""
        vehicles = obj.prognosisvindetails_set.all()[:5]  # Limit to first 5
        return [
            {
                'id': v.id,
                'vin_no': v.vin_no,
                'location': v.vehicle_location
            } for v in vehicles
        ]
    
    def get_errors_summary(self, obj):
        """Get summary of error types in this ticket"""
        errors = obj.prognosticketerrorcode_set.all()[:5]  # Limit to first 5
        return [
            {
                'error_id': e.error_id,
                'error_type': e.error_type,
                'status': e.error_status
            } for e in errors
        ]

class TicketDetailSerializer(serializers.ModelSerializer):
    """Detailed serializer for single ticket view"""
    
    vehicles = VehicleDetailSerializer(source='prognosisvindetails_set', many=True, read_only=True)
    error_codes = ErrorCodeDetailSerializer(source='prognosticketerrorcode_set', many=True, read_only=True)
    status_display = serializers.SerializerMethodField()
    summary = serializers.SerializerMethodField()
    
    class Meta:
        model = PrognosisTicket
        fields = [
            'id', 'customer_id', 'alert_count', 'vehicle_count',
            'call_status_id', 'status_display', 'call_category_id',
            'remarks', 'customer_complaint', 'updated_by',
            'created_at', 'updated_at', 'vehicles', 'error_codes', 'summary'
        ]
    
    def get_status_display(self, obj):
        """Get human-readable status"""
        status_map = {
            1: 'Open',
            2: 'In Progress', 
            3: 'Resolved',
            4: 'Closed',
            5: 'Cancelled'
        }
        return status_map.get(obj.call_status_id, 'Unknown')
    
    def get_summary(self, obj):
        """Get ticket summary statistics"""
        vehicles = obj.prognosisvindetails_set.all()
        error_codes = obj.prognosticketerrorcode_set.all()
        
        # Error status breakdown
        error_status_counts = {}
        for error in error_codes:
            status = error.error_status or 'UNKNOWN'
            error_status_counts[status] = error_status_counts.get(status, 0) + 1
        
        # Unique error types
        unique_error_types = list(set(
            error.error_type for error in error_codes if error.error_type
        ))
        
        # Location distribution
        locations = [v.vehicle_location for v in vehicles if v.vehicle_location]
        unique_locations = list(set(locations))
        
        return {
            'total_vehicles': vehicles.count(),
            'total_errors': error_codes.count(),
            'unique_error_types': len(unique_error_types),
            'error_types': unique_error_types[:10],  # Limit to first 10
            'error_status_breakdown': error_status_counts,
            'unique_locations': len(unique_locations),
            'locations': unique_locations[:5],  # Limit to first 5
            'latest_error_time': max([e.created_at for e in error_codes]) if error_codes else None
        }

class TicketFilterSerializer(serializers.Serializer):
    """Serializer for ticket filtering parameters"""
    
    customer_id = serializers.IntegerField(required=False, min_value=1)
    call_status_id = serializers.IntegerField(required=False, min_value=1, max_value=10)
    date_from = serializers.DateField(required=False)
    date_to = serializers.DateField(required=False)
    min_vehicles = serializers.IntegerField(required=False, min_value=1)
    max_vehicles = serializers.IntegerField(required=False, min_value=1)
    min_alerts = serializers.IntegerField(required=False, min_value=1)
    max_alerts = serializers.IntegerField(required=False, min_value=1)
    search = serializers.CharField(required=False, max_length=100)
    
    def validate_search(self, value):
        """Validate search parameter to prevent injection"""
        if value:
            # Remove potentially dangerous characters
            cleaned = re.sub(r'[<>"\';\\]', '', value.strip())
            if len(cleaned) < 2:
                raise serializers.ValidationError("Search term must be at least 2 characters")
            return cleaned[:100]  # Limit length
        return value
    
    def validate(self, data):
        """Cross-field validation"""
        # Validate date range
        if data.get('date_from') and data.get('date_to'):
            if data['date_from'] > data['date_to']:
                raise serializers.ValidationError("date_from must be before date_to")
            
            # Limit date range to prevent excessive queries
            date_diff = (data['date_to'] - data['date_from']).days
            if date_diff > 365:
                raise serializers.ValidationError("Date range cannot exceed 365 days")
        
        # Validate vehicle count range
        if data.get('min_vehicles') and data.get('max_vehicles'):
            if data['min_vehicles'] > data['max_vehicles']:
                raise serializers.ValidationError("min_vehicles must be less than max_vehicles")
        
        # Validate alert count range
        if data.get('min_alerts') and data.get('max_alerts'):
            if data['min_alerts'] > data['max_alerts']:
                raise serializers.ValidationError("min_alerts must be less than max_alerts")
        
        return data