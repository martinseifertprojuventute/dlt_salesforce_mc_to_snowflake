"""Salesforce Marketing Cloud pipeline using both REST and SOAP APIs."""

import dlt
import requests
from typing import Iterator, Dict, Any, Optional, List
from datetime import datetime, timedelta
from dlt.sources.rest_api import rest_api_source


def get_access_token(subdomain: str, client_id: str, client_secret: str) -> str:
    """
    Obtain OAuth2 access token using client credentials flow.

    Args:
        subdomain: Your unique Marketing Cloud subdomain
        client_id: OAuth2 client ID from API Integration
        client_secret: OAuth2 client secret from API Integration

    Returns:
        Access token valid for approximately 20 minutes
    """
    token_url = f"https://{subdomain}.auth.marketingcloudapis.com/v2/token"

    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    headers = {
        "Content-Type": "application/json",
    }

    response = requests.post(token_url, json=payload, headers=headers)
    response.raise_for_status()

    token_data = response.json()
    return token_data["access_token"]


def create_soap_resource(
    object_type: str,
    subdomain: str,
    client_id: str,
    client_secret: str,
    properties: List[str],
    filter_property: Optional[str] = None,
    filter_operator: str = "greaterThan",
    days_back: int = 4,
    full_load: bool = False,
    primary_key: str = "id",
):
    """
    Factory function to create a generic SOAP API resource for any object type.

    Args:
        object_type: SOAP object type (e.g., 'SentEvent', 'Send', 'Email')
        subdomain: Marketing Cloud subdomain
        client_id: OAuth2 client ID
        client_secret: OAuth2 client secret
        properties: List of properties to retrieve from the object
        filter_property: Property to filter on (e.g., 'EventDate', 'CreatedDate', 'ModifiedDate')
        filter_operator: Filter operator ('greaterThan', 'equals', 'lessThan', etc.)
        days_back: Number of days to look back (ignored if full_load=True)
        full_load: If True, retrieve all objects without date filter
        primary_key: Primary key field for merge mode

    Returns:
        A dlt resource function
    """

    @dlt.resource(
        name=object_type.lower() + 's',
        write_disposition="merge",
        primary_key=primary_key
    )
    def soap_resource() -> Iterator[Dict[str, Any]]:
        """Generic SOAP API resource that retrieves objects based on configuration."""
        try:
            from zeep import Client
            from zeep.transports import Transport
            from requests import Session
            from lxml import etree
            import zeep.exceptions
        except ImportError:
            raise ImportError(
                "zeep is required for SOAP API access. "
                "Install it with: pip install zeep lxml"
            )

        # SOAP API endpoint
        wsdl_url = f"https://{subdomain}.soap.marketingcloudapis.com/etframework.wsdl"

        # Create session
        session = Session()
        transport = Transport(session=session)

        if full_load:
            print(f"Fetching all {object_type} objects (full load)...")
        else:
            print(f"Fetching {object_type} objects from the last {days_back} days...")
        print(f"Connecting to SOAP WSDL: {wsdl_url}")

        # Create refreshable OAuth plugin
        class RefreshableOAuthPlugin:
            def __init__(self, subdomain, client_id, client_secret):
                self.subdomain = subdomain
                self.client_id = client_id
                self.client_secret = client_secret
                self.token = self.refresh_token()

            def refresh_token(self):
                """Get a fresh OAuth2 token."""
                print(f"Refreshing OAuth2 token for {object_type}...")
                new_token = get_access_token(self.subdomain, self.client_id, self.client_secret)
                self.token = new_token
                return new_token

            def egress(self, envelope, http_headers, operation, binding_options):
                header = envelope.find('{http://schemas.xmlsoap.org/soap/envelope/}Header')
                if header is None:
                    header = etree.Element('{http://schemas.xmlsoap.org/soap/envelope/}Header')
                    envelope.insert(0, header)

                fueloauth = etree.SubElement(header, 'fueloauth')
                fueloauth.set('xmlns', 'http://exacttarget.com')
                fueloauth.text = self.token

                return envelope, http_headers

            def ingress(self, envelope, http_headers, operation):
                return envelope, http_headers

        # Helper function to execute SOAP request with auto-retry on token expiration
        def execute_with_retry(client, oauth_plugin, request_func, max_retries=2):
            """Execute a SOAP request with automatic token refresh on expiration."""
            for attempt in range(max_retries):
                try:
                    return request_func()
                except zeep.exceptions.Fault as e:
                    if 'Token Expired' in str(e) and attempt < max_retries - 1:
                        print(f"Token expired, refreshing... (attempt {attempt + 1}/{max_retries})")
                        oauth_plugin.refresh_token()
                        # Recreate client with new token
                        client._binding_options = {}  # Reset binding options
                    else:
                        raise

        try:
            # Create OAuth plugin with refresh capability
            oauth_plugin = RefreshableOAuthPlugin(subdomain, client_id, client_secret)

            # Create SOAP client
            client = Client(wsdl=wsdl_url, transport=transport, plugins=[oauth_plugin])

            # Build retrieve request based on full_load and filter settings
            if full_load or not filter_property:
                # Retrieve all objects without filter
                retrieve_request = client.get_type('ns0:RetrieveRequest')(
                    ObjectType=object_type,
                    Properties=properties
                )
            else:
                # Filter by date for incremental loads
                start_date = datetime.now() - timedelta(days=days_back)
                filter_part = client.get_type('ns0:SimpleFilterPart')(
                    Property=filter_property,
                    SimpleOperator=filter_operator,
                    DateValue=[start_date]
                )
                retrieve_request = client.get_type('ns0:RetrieveRequest')(
                    ObjectType=object_type,
                    Properties=properties,
                    Filter=filter_part
                )

            # Execute retrieve with token refresh retry
            print(f"Executing SOAP Retrieve request for {object_type} objects...")
            response = execute_with_retry(
                client,
                oauth_plugin,
                lambda: client.service.Retrieve(RetrieveRequest=retrieve_request)
            )

            # Debug output
            print(f"Response status: {response.Status if hasattr(response, 'Status') else 'Unknown'}")
            if hasattr(response, 'OverallStatus'):
                print(f"Overall Status: {response.OverallStatus}")
            if hasattr(response, 'RequestID'):
                print(f"Request ID: {response.RequestID}")

            # Check for results
            if hasattr(response, 'Results'):
                if response.Results:
                    print(f"Number of results: {len(response.Results)}")
                else:
                    print(f"Results is empty or None")
                    if hasattr(response, 'StatusMessage'):
                        print(f"Status Message: {response.StatusMessage}")

            count = 0
            request_id = response.RequestID if hasattr(response, 'RequestID') else None
            has_more_data = False

            # Process first batch
            if hasattr(response, 'Results') and response.Results:
                # print(f"Processing {len(response.Results)} {object_type} records...")
                for obj in response.Results:
                    count += 1

                    # Convert object to dictionary dynamically
                    obj_dict = {}
                    for prop in properties:
                        # Handle nested properties like 'Email.ID'
                        if '.' in prop:
                            parts = prop.split('.')
                            value = obj
                            for part in parts:
                                if hasattr(value, part):
                                    value = getattr(value, part, None)
                                else:
                                    value = None
                                    break
                            # Use the last part as the key name
                            key = '_'.join(parts).lower()
                            obj_dict[key] = value
                        else:
                            value = getattr(obj, prop, None)
                            key = prop.lower()

                            # Convert datetime objects to ISO format
                            if value and isinstance(value, datetime):
                                value = value.isoformat()

                            obj_dict[key] = value

                    # Generate ID for objects that don't have one
                    if 'id' not in obj_dict or not obj_dict['id']:
                        # For Event objects, create composite ID from SendID, SubscriberKey, and EventDate
                        if object_type in ['SentEvent', 'BounceEvent', 'ClickEvent', 'OpenEvent', 'UnsubEvent']:
                            sendid = obj_dict.get('sendid', 'unknown')
                            subscriberkey = obj_dict.get('subscriberkey', 'unknown')
                            eventdate = obj_dict.get('eventdate', datetime.now().isoformat())
                            # For ClickEvent, include URL to make it unique (same subscriber can click multiple links)
                            if object_type == 'ClickEvent':
                                urlid = obj_dict.get('urlid', 'unknown')
                                obj_dict['id'] = f"{sendid}_{subscriberkey}_{urlid}_{eventdate}"
                            else:
                                obj_dict['id'] = f"{sendid}_{subscriberkey}_{eventdate}"
                        # For Subscriber objects, use SubscriberKey as id
                        elif object_type == 'Subscriber' and 'subscriberkey' in obj_dict:
                            obj_dict['id'] = obj_dict['subscriberkey']

                    yield obj_dict

            # Check for more data
            if hasattr(response, 'OverallStatus'):
                has_more_data = response.OverallStatus == 'MoreDataAvailable'

            # Paginate through remaining results
            while has_more_data and request_id:
                print(f"Fetching more {object_type} data (retrieved {count} so far)...")

                continue_request = client.get_type('ns0:RetrieveRequest')(
                    ContinueRequest=request_id,
                    ObjectType=object_type
                )

                response = execute_with_retry(
                    client,
                    oauth_plugin,
                    lambda: client.service.Retrieve(RetrieveRequest=continue_request)
                )

                if hasattr(response, 'Results') and response.Results:
                    # print(f"Processing {len(response.Results)} additional {object_type} records...")
                    for obj in response.Results:
                        count += 1

                        # Convert object to dictionary dynamically
                        obj_dict = {}
                        for prop in properties:
                            # Handle nested properties
                            if '.' in prop:
                                parts = prop.split('.')
                                value = obj
                                for part in parts:
                                    if hasattr(value, part):
                                        value = getattr(value, part, None)
                                    else:
                                        value = None
                                        break
                                key = '_'.join(parts).lower()
                                obj_dict[key] = value
                            else:
                                value = getattr(obj, prop, None)
                                key = prop.lower()

                                # Convert datetime objects
                                if value and isinstance(value, datetime):
                                    value = value.isoformat()

                                obj_dict[key] = value

                        # Generate ID if needed
                        if 'id' not in obj_dict or not obj_dict['id']:
                            # For Event objects, create composite ID from SendID, SubscriberKey, and EventDate
                            if object_type in ['SentEvent', 'BounceEvent', 'ClickEvent', 'OpenEvent', 'UnsubEvent']:
                                sendid = obj_dict.get('sendid', 'unknown')
                                subscriberkey = obj_dict.get('subscriberkey', 'unknown')
                                eventdate = obj_dict.get('eventdate', datetime.now().isoformat())
                                # For ClickEvent, include URL to make it unique (same subscriber can click multiple links)
                                if object_type == 'ClickEvent':
                                    urlid = obj_dict.get('urlid', 'unknown')
                                    obj_dict['id'] = f"{sendid}_{subscriberkey}_{urlid}_{eventdate}"
                                else:
                                    obj_dict['id'] = f"{sendid}_{subscriberkey}_{eventdate}"
                            # For Subscriber objects, use SubscriberKey as id
                            elif object_type == 'Subscriber' and 'subscriberkey' in obj_dict:
                                obj_dict['id'] = obj_dict['subscriberkey']

                        yield obj_dict

                has_more_data = hasattr(response, 'OverallStatus') and response.OverallStatus == 'MoreDataAvailable'
                request_id = response.RequestID if hasattr(response, 'RequestID') else None

            print(f"Retrieved {count} {object_type} records in total")

        except Exception as e:
            print(f"Error fetching {object_type} via SOAP API: {e}")
            print(f"Error type: {type(e).__name__}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            print(f"Skipping {object_type}...")
            return

    return soap_resource


@dlt.source
def salesforce_marketing_cloud_rest_source(
    subdomain: str = dlt.secrets.value,
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
):
    """
    Salesforce Marketing Cloud REST API source.

    Args:
        subdomain: Your unique Marketing Cloud subdomain
        client_id: OAuth2 client ID from API Integration
        client_secret: OAuth2 client secret from API Integration

    Returns:
        REST API source with Assets, Campaigns, and Journeys endpoints

    Note:
        The REST API token expires after ~20 minutes. For long-running extractions,
        you may need to re-run the pipeline if it fails with 401 Unauthorized.
        The SOAP API has built-in token refresh.
    """
    # Get OAuth2 access token
    print("Generating OAuth2 token for REST API...")
    access_token = get_access_token(subdomain, client_id, client_secret)
    print("Got it. REST now...")

    # REST API base URL
    base_url = f"https://{subdomain}.rest.marketingcloudapis.com"

    # REST API configuration
    config = {
        "client": {
            "base_url": base_url,
            "auth": {
                "type": "bearer",
                "token": access_token,
            },
        },
        "resource_defaults": {
            "write_disposition": "replace",  # Full load for all REST resources
        },
        "resources": [
            {
                "name": "assets",
                "columns": {
                    "data": {"data_type": "json"},
                    "file_properties": {"data_type": "json"},
                    "legacy_data": {"data_type": "json"},
                    "meta": {"data_type": "json"},
                    "slots": {"data_type": "json"},
                },
                "endpoint": {
                    "path": "asset/v1/content/assets",
                    "params": {
                        "$pagesize": 50,
                    },
                    "paginator": {
                        "type": "page_number",
                        "page_param": "$page",
                        "total_path": "count",
                        "base_page": 1,
                    },
                },
            },
            {
                "name": "campaigns",
                "endpoint": {
                    "path": "hub/v1/campaigns",
                    "params": {
                        "$pagesize": 50,
                    },
                    "paginator": {
                        "type": "page_number",
                        "page_param": "$page",
                        "total_path": "count",
                        "base_page": 1,
                    },
                },
            },
            {
                "name": "journeys",
                "columns": {
                    "meta_data": {"data_type": "json"},
                    "stats": {"data_type": "json"},
                },
                "endpoint": {
                    "path": "interaction/v1/interactions",
                    "params": {
                        "$pagesize": 50,
                    },
                    "paginator": {
                        "type": "page_number",
                        "page_param": "$page",
                        "total_path": "count",
                        "base_page": 1,
                    },
                },
            },
        ],
    }

    return rest_api_source(
        config,
        max_table_nesting=0,   # All nested data stays as JSON
    )


@dlt.source
def salesforce_marketing_cloud_soap_source(
    subdomain: str = dlt.secrets.value,
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    days_back: int = 4,
):
    """
    Salesforce Marketing Cloud SOAP API source with configurable object types.

    Args:
        subdomain: Your unique Marketing Cloud subdomain
        client_id: OAuth2 client ID from API Integration
        client_secret: OAuth2 client secret from API Integration
        days_back: Number of days to look back for date-filtered objects (default: 28)

    Note:
        Each object's full_load setting is configured in the soap_objects list below.
        Set full_load=True to retrieve all records without date filter.
        Set full_load=False to use filter_property with days_back parameter.
    """

    # Define SOAP objects to retrieve with their configurations
    soap_objects = [
        {
            'object_type': 'BounceEvent',
            'properties': ['SendID', 'SubscriberKey', 'EventDate', 'EventType', 'BounceCategory', 'BounceType', 'SMTPCode', 'SMTPReason', 'BatchID', 'TriggeredSendDefinitionObjectID'],
            'filter_property': 'EventDate',
            'days_back': days_back,
            'full_load': False,  # Use date filter - high volume object
            'primary_key': 'id'
        },
        {
            'object_type': 'ClickEvent',
            'properties': ['SendID', 'SubscriberKey', 'EventDate', 'EventType', 'URL', 'URLID', 'BatchID', 'TriggeredSendDefinitionObjectID'],
            'filter_property': 'EventDate',
            'days_back': days_back,
            'full_load': False,  # Use date filter - high volume object
            'primary_key': 'id'
        },
        {
            'object_type': 'OpenEvent',
            'properties': ['SendID', 'SubscriberKey', 'EventDate', 'EventType', 'BatchID', 'TriggeredSendDefinitionObjectID'],
            'filter_property': 'EventDate',
            'days_back': days_back,
            'full_load': False,  # Use date filter - high volume object
            'primary_key': 'id'
        },
        {
            'object_type': 'Send',
            'properties': ['ID', 'CreatedDate', 'ModifiedDate', 'Client.ID', 'Email.ID', 'SendDate', 'FromName', 'FromAddress', 'Status', 'Subject', 'EmailName', 'NumberSent', 'NumberDelivered', 'NumberTargeted', 'NumberErrored', 'NumberExcluded', 'PreviewURL'],
            'filter_property': 'CreatedDate',
            'days_back': days_back,
            'full_load': False,  # Use date filter
            'primary_key': 'id'
        },
        {
            'object_type': 'SentEvent',
            'properties': ['SendID', 'SubscriberKey', 'EventDate', 'EventType', 'ListID', 'BatchID', 'TriggeredSendDefinitionObjectID'],
            'filter_property': 'EventDate',
            'days_back': days_back,
            'full_load': False,  # Use date filter - high volume object
            'primary_key': 'id'
        },
        {
            'object_type': 'Subscriber',
            'properties': ['ID', 'SubscriberKey', 'EmailAddress', 'Status', 'CreatedDate', 'EmailTypePreference', 'UnsubscribedDate'],
            'filter_property': None,  # Subscriber doesn't support date filtering via ModifiedDate
            'days_back': days_back,
            'full_load': True,  # Must do full load - Subscriber doesn't support ModifiedDate filter
            'primary_key': 'subscriberkey'  # SubscriberKey is the proper primary key for Subscriber objects
        },
        {
            'object_type': 'UnsubEvent',
            'properties': ['SendID', 'SubscriberKey', 'EventDate', 'EventType', 'IsMasterUnsubscribed', 'BatchID', 'TriggeredSendDefinitionObjectID'],
            'filter_property': 'EventDate',
            'days_back': days_back,
            'full_load': False,  # Use date filter - high volume object
            'primary_key': 'id'
        },
    ]

    # Yield resources for each configured object type
    for config in soap_objects:
        yield create_soap_resource(
            object_type=config['object_type'],
            subdomain=subdomain,
            client_id=client_id,
            client_secret=client_secret,
            properties=config['properties'],
            filter_property=config.get('filter_property'),
            days_back=config['days_back'],
            full_load=config['full_load'],
            primary_key=config['primary_key']
        )


pipeline = dlt.pipeline(
    pipeline_name='salesforce_marketing_cloud_pipeline',
    destination='snowflake',
    dataset_name='salesforce_marketing_cloud',
    # dev_mode=True,            # use separate timestamped schema
    # refresh="drop_sources"    # drop tables AND reset state for full reload (only needed once)
)


if __name__ == "__main__":
    # Load REST API data (Assets, Campaigns, Journeys)
    print("Loading REST API data...")
    load_info = pipeline.run(salesforce_marketing_cloud_rest_source())
    print(load_info)

    # Load SOAP API data (SentEvent, Send, Email)
    print("\nLoading SOAP API data...")
    load_info = pipeline.run(salesforce_marketing_cloud_soap_source())
    print(load_info)