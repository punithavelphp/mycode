# prognosis/retrieve_views.py
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.throttling import UserRateThrottle
from rest_framework.pagination import PageNumberPagination
from django.db.models import Q, Count, Prefetch
from django.core.exceptions import ValidationError
from datetime import datetime, timedelta
from .models import PrognosisTicket, PrognosisVinDetails, PrognosisTicketErrorcode
from .retrieve_serializers import (
    TicketListSerializer, 
    TicketDetailSerializer,
    TicketFilterSerializer
)
import logging
import re

logger = logging.getLogger(__name__)

class PrognosisRetrieveRateThrottle(UserRateThrottle):
    scope = 'prognosis_retrieve'
    rate = '500/hour'

class TicketPagination(PageNumberPagination):
    page_size = 20
    page_size_query_param = 'page_size'
    max_page_size = 100

class TicketListView(APIView):
    """
    API to get list of tickets with basic information
    GET /api/prognosis/tickets/
    """
    
    permission_classes = [IsAuthenticated]
    throttle_classes = [PrognosisRetrieveRateThrottle]
    pagination_class = TicketPagination
    
    def get(self, request):
        try:
            # Validate query parameters
            filter_serializer = TicketFilterSerializer(data=request.query_params)
            if not filter_serializer.is_valid():
                return Response({
                    'success': False,
                    'message': 'Invalid filter parameters',
                    'errors': filter_serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST)
            
            filters = filter_serializer.validated_data
            
            # Build queryset with optimized queries
            queryset = PrognosisTicket.objects.select_related().prefetch_related(
                'prognosisvindetails_set',
                'prognosticketerrorcode_set'
            ).annotate(
                vehicle_count_actual=Count('prognosisvindetails', distinct=True),
                error_count=Count('prognosticketerrorcode', distinct=True)
            )
            
            # Apply filters securely
            queryset = self.apply_filters(queryset, filters)
            
            # Apply pagination
            paginator = self.pagination_class()
            paginated_queryset = paginator.paginate_queryset(queryset, request)
            
            # Serialize data
            serializer = TicketListSerializer(paginated_queryset, many=True)
            
            return paginator.get_paginated_response({
                'success': True,
                'tickets': serializer.data
            })
            
        except Exception as e:
            logger.error(f"Error retrieving tickets: {str(e)}")
            return Response({
                'success': False,
                'message': 'Error retrieving tickets'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def apply_filters(self, queryset, filters):
        """Apply filters securely to the queryset"""
        
        # Customer ID filter
        if filters.get('customer_id'):
            queryset = queryset.filter(customer_id=filters['customer_id'])
        
        # Status filter
        if filters.get('call_status_id'):
            queryset = queryset.filter(call_status_id=filters['call_status_id'])
        
        # Date range filter
        if filters.get('date_from'):
            queryset = queryset.filter(created_at__gte=filters['date_from'])
        
        if filters.get('date_to'):
            # Add one day to include the entire end date
            end_date = filters['date_to'] + timedelta(days=1)
            queryset = queryset.filter(created_at__lt=end_date)
        
        # Vehicle count filter
        if filters.get('min_vehicles'):
            queryset = queryset.filter(vehicle_count__gte=filters['min_vehicles'])
        
        if filters.get('max_vehicles'):
            queryset = queryset.filter(vehicle_count__lte=filters['max_vehicles'])
        
        # Alert count filter
        if filters.get('min_alerts'):
            queryset = queryset.filter(alert_count__gte=filters['min_alerts'])
        
        if filters.get('max_alerts'):
            queryset = queryset.filter(alert_count__lte=filters['max_alerts'])
        
        # Search in remarks (secure text search)
        if filters.get('search'):
            search_term = filters['search']
            queryset = queryset.filter(
                Q(remarks__icontains=search_term) |
                Q(customer_complaint__icontains=search_term)
            )
        
        return queryset.order_by('-created_at')

class TicketDetailView(APIView):
    """
    API to get detailed ticket information with vehicles and error codes
    GET /api/prognosis/tickets/{ticket_id}/
    """
    
    permission_classes = [IsAuthenticated]
    throttle_classes = [PrognosisRetrieveRateThrottle]
    
    def get(self, request, ticket_id):
        try:
            # Validate ticket_id
            if not self.validate_ticket_id(ticket_id):
                return Response({
                    'success': False,
                    'message': 'Invalid ticket ID format'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Get ticket with related data
            try:
                ticket = PrognosisTicket.objects.select_related().prefetch_related(
                    Prefetch(
                        'prognosisvindetails_set',
                        queryset=PrognosisVinDetails.objects.select_related()
                    ),
                    Prefetch(
                        'prognosticketerrorcode_set',
                        queryset=PrognosisTicketErrorcode.objects.select_related()
                    )
                ).get(id=ticket_id)
                
            except PrognosisTicket.DoesNotExist:
                return Response({
                    'success': False,
                    'message': 'Ticket not found'
                }, status=status.HTTP_404_NOT_FOUND)
            
            # Serialize detailed data
            serializer = TicketDetailSerializer(ticket)
            
            return Response({
                'success': True,
                'ticket': serializer.data
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Error retrieving ticket {ticket_id}: {str(e)}")
            return Response({
                'success': False,
                'message': 'Error retrieving ticket details'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def validate_ticket_id(self, ticket_id):
        """Validate ticket_id is a positive integer"""
        try:
            return isinstance(ticket_id, int) or (
                isinstance(ticket_id, str) and 
                ticket_id.isdigit() and 
                int(ticket_id) > 0
            )
        except (ValueError, TypeError):
            return False

class TicketsByCustomerView(APIView):
    """
    API to get tickets for a specific customer
    GET /api/prognosis/customers/{customer_id}/tickets/
    """
    
    permission_classes = [IsAuthenticated]
    throttle_classes = [PrognosisRetrieveRateThrottle]
    pagination_class = TicketPagination
    
    def get(self, request, customer_id):
        try:
            # Validate customer_id
            if not self.validate_customer_id(customer_id):
                return Response({
                    'success': False,
                    'message': 'Invalid customer ID format'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Get tickets for customer
            queryset = PrognosisTicket.objects.filter(
                customer_id=customer_id
            ).select_related().prefetch_related(
                'prognosisvindetails_set',
                'prognosticketerrorcode_set'
            ).annotate(
                vehicle_count_actual=Count('prognosisvindetails', distinct=True),
                error_count=Count('prognosticketerrorcode', distinct=True)
            ).order_by('-created_at')
            
            if not queryset.exists():
                return Response({
                    'success': True,
                    'message': 'No tickets found for this customer',
                    'tickets': []
                }, status=status.HTTP_200_OK)
            
            # Apply pagination
            paginator = self.pagination_class()
            paginated_queryset = paginator.paginate_queryset(queryset, request)
            
            # Serialize data
            serializer = TicketListSerializer(paginated_queryset, many=True)
            
            return paginator.get_paginated_response({
                'success': True,
                'customer_id': customer_id,
                'tickets': serializer.data
            })
            
        except Exception as e:
            logger.error(f"Error retrieving tickets for customer {customer_id}: {str(e)}")
            return Response({
                'success': False,
                'message': 'Error retrieving customer tickets'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def validate_customer_id(self, customer_id):
        """Validate customer_id is a positive integer"""
        try:
            return isinstance(customer_id, int) or (
                isinstance(customer_id, str) and 
                customer_id.isdigit() and 
                int(customer_id) > 0
            )
        except (ValueError, TypeError):
            return False

class TicketStatsView(APIView):
    """
    API to get ticket statistics
    GET /api/prognosis/tickets/stats/
    """
    
    permission_classes = [IsAuthenticated]
    throttle_classes = [PrognosisRetrieveRateThrottle]
    
    def get(self, request):
        try:
            # Get date range from query params (default to last 30 days)
            days = request.query_params.get('days', 30)
            
            # Validate days parameter
            try:
                days = int(days)
                if days <= 0 or days > 365:
                    days = 30
            except (ValueError, TypeError):
                days = 30
            
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Get statistics
            stats = self.calculate_ticket_stats(start_date, end_date)
            
            return Response({
                'success': True,
                'period': f'Last {days} days',
                'stats': stats
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Error calculating ticket stats: {str(e)}")
            return Response({
                'success': False,
                'message': 'Error calculating statistics'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def calculate_ticket_stats(self, start_date, end_date):
        """Calculate ticket statistics for the given date range"""
        base_queryset = PrognosisTicket.objects.filter(
            created_at__gte=start_date,
            created_at__lte=end_date
        )
        
        # Basic counts
        total_tickets = base_queryset.count()
        total_vehicles = PrognosisVinDetails.objects.filter(
            prognosis_ticket__in=base_queryset
        ).count()
        total_errors = PrognosisTicketErrorcode.objects.filter(
            ticket__in=base_queryset
        ).count()
        
        # Status breakdown
        status_breakdown = {}
        for status_id in [1, 2, 3, 4, 5]:  # Adjust based on your status values
            count = base_queryset.filter(call_status_id=status_id).count()
            if count > 0:
                status_breakdown[f'status_{status_id}'] = count
        
        # Average metrics
        avg_vehicles_per_ticket = total_vehicles / total_tickets if total_tickets > 0 else 0
        avg_errors_per_ticket = total_errors / total_tickets if total_tickets > 0 else 0
        
        return {
            'total_tickets': total_tickets,
            'total_vehicles': total_vehicles,
            'total_errors': total_errors,
            'avg_vehicles_per_ticket': round(avg_vehicles_per_ticket, 2),
            'avg_errors_per_ticket': round(avg_errors_per_ticket, 2),
            'status_breakdown': status_breakdown
        }