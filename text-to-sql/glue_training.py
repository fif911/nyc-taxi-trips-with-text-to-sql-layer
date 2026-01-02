"""
Glue Data Catalog Training Service for Vanna 2.0

This module handles training Vanna 2.0's knowledge base with schemas from
AWS Glue Data Catalog, including DDLs, business context, and sample queries.
"""
import boto3
from typing import List, Dict, Set, Optional, Tuple
from pathlib import Path
from config import Config


class GlueTrainingService:
    """Service for training Vanna (legacy or 2.0) with Glue Data Catalog schemas"""
    
    def __init__(self, vanna_instance, glue_client: Optional[boto3.client] = None):
        """
        Initialize Glue training service
        
        Args:
            vanna_instance: Vanna instance (legacy VannaDefault or Vanna 2.0 Agent)
            glue_client: Optional boto3 Glue client (creates one if not provided)
        """
        self.vanna = vanna_instance  # Can be legacy or 2.0 agent
        self.glue = glue_client or boto3.client('glue', region_name=Config.AWS_REGION)
        self.cache_file = Path(__file__).parent / '.glue_tables_cache.txt'
    
    def get_glue_tables(self) -> List[Dict]:
        """Fetch all tables from Glue Data Catalog"""
        try:
            kwargs = {'DatabaseName': Config.GLUE_DATABASE}
            if Config.GLUE_CATALOG_ID:
                kwargs['CatalogId'] = Config.GLUE_CATALOG_ID
            
            response = self.glue.get_tables(**kwargs)
            return response['TableList']
        except Exception as e:
            print(f"Error fetching Glue tables: {e}")
            return []
    
    def generate_ddl_from_glue(self, table_name: str) -> Optional[str]:
        """Generate DDL from Glue table metadata"""
        try:
            kwargs = {
                'DatabaseName': Config.GLUE_DATABASE,
                'Name': table_name
            }
            if Config.GLUE_CATALOG_ID:
                kwargs['CatalogId'] = Config.GLUE_CATALOG_ID
            
            response = self.glue.get_table(**kwargs)
            table = response['Table']
            storage = table['StorageDescriptor']
            
            # Build DDL
            ddl = f"CREATE EXTERNAL TABLE {table_name} (\n"
            
            # Add columns
            columns = []
            for col in storage['Columns']:
                col_type = col['Type']
                col_name = col['Name']
                columns.append(f"    {col_name} {col_type}")
            
            ddl += ",\n".join(columns)
            ddl += "\n)"
            
            # Add partitions if they exist
            if 'PartitionKeys' in table and table['PartitionKeys']:
                partition_cols = []
                for part in table['PartitionKeys']:
                    partition_cols.append(f"{part['Name']} {part['Type']}")
                ddl += f"\nPARTITIONED BY ({', '.join(partition_cols)})"
            
            # Add storage format
            serde_info = storage.get('SerdeInfo', {})
            serialization_lib = serde_info.get(
                'SerializationLibrary',
                'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            )
            
            # Map to Athena format
            if 'parquet' in serialization_lib.lower():
                storage_format = 'PARQUET'
            elif 'json' in serialization_lib.lower():
                storage_format = 'JSON'
            else:
                storage_format = 'PARQUET'
            
            ddl += f"\nSTORED AS {storage_format}"
            ddl += f"\nLOCATION '{storage['Location']}'"
            
            return ddl
        
        except Exception as e:
            print(f"Error generating DDL for {table_name}: {e}")
            return None
    
    def _load_cached_table_names(self) -> Set[str]:
        """Load cached table names from file"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    return set(line.strip() for line in f if line.strip())
            except Exception:
                pass
        return set()
    
    def _save_cached_table_names(self, table_names: Set[str]):
        """Save table names to cache file"""
        try:
            with open(self.cache_file, 'w') as f:
                for name in sorted(table_names):
                    f.write(f"{name}\n")
        except Exception as e:
            print(f"Error saving cache: {e}")
    
    def _has_existing_training_data(self) -> bool:
        """Check if Vanna agent already has training data"""
        # In Vanna 2.0, we can check if the knowledge base has items
        # This is a simplified check - Vanna 2.0 may have different APIs
        # If the agent has a way to check knowledge base, use it here
        # For now, we'll rely on the table cache to determine if training is needed
        return False  # Always check for new tables
    
    def _check_for_new_tables(self) -> Tuple[bool, List[str], Set[str]]:
        """Check if Glue catalog has new tables compared to cached data"""
        current_tables = self.get_glue_tables()
        current_table_names = {table['Name'] for table in current_tables}
        cached_table_names = self._load_cached_table_names()
        
        new_tables = current_table_names - cached_table_names
        has_new = len(new_tables) > 0 or len(current_table_names) != len(cached_table_names)
        
        return has_new, list(new_tables), current_table_names
    
    def train_from_glue_catalog(self, force_refresh: bool = False) -> Dict[str, any]:
        """
        Train Vanna agent with schemas from Glue Data Catalog
        
        Args:
            force_refresh: If True, force retraining even if cached data exists
            
        Returns:
            Dict with training results
        """
        # Check for new tables
        has_new_tables, new_table_names, all_table_names = self._check_for_new_tables()
        
        if not force_refresh and not has_new_tables and all_table_names:
            # Check if we have cached table names and no new tables
            if self.cache_file.exists():
                print("✅ Using cached training data - no new tables detected")
                return {
                    'trained': True,
                    'tables_count': len(all_table_names),
                    'new_tables': []
                }
        
        # Fetch all tables
        tables = self.get_glue_tables()
        if not tables:
            print("⚠️ No tables found in Glue Data Catalog")
            return {
                'trained': False,
                'tables_count': 0,
                'error': 'No tables found'
            }
        
        print(f"🔄 Training on {len(tables)} tables from Glue Data Catalog...")
        
        # Train on DDLs
        ddl_count = 0
        for table in tables:
            table_name = table['Name']
            ddl = self.generate_ddl_from_glue(table_name)
            if ddl:
                try:
                    # Use legacy train() method (works with both legacy and 2.0 via adapter)
                    # Add database qualification to DDL
                    db_quoted = f'"{Config.ATHENA_DATABASE}"'
                    ddl_qualified = ddl.replace(f"CREATE EXTERNAL TABLE {table_name}", 
                                              f"CREATE EXTERNAL TABLE {db_quoted}.{table_name}")
                    
                    self.vanna.train(ddl=ddl_qualified)
                    ddl_count += 1
                    if ddl_count % 5 == 0:
                        print(f"  Added {ddl_count} DDLs so far...")
                except Exception as e:
                    print(f"Error adding DDL for {table_name}: {e}")
                    import traceback
                    traceback.print_exc()
        
        # Train business context
        self._train_business_context()
        
        # Train sample queries
        self._train_sample_queries()
        
        # Save cache
        self._save_cached_table_names(all_table_names)
        
        print(f"✅ Trained on {ddl_count} tables from Glue Data Catalog")
        return {
            'trained': True,
            'tables_count': ddl_count,
            'new_tables': new_table_names
        }
    
    def _train_business_context(self):
        """Train with business terminology and context"""
        db = f'"{Config.ATHENA_DATABASE}"'
        documentation = f"""
NYC Taxi Trip Data - Business Context:

Database: {db} (use double quotes for database and table names)

Tables (use fully qualified names with double quotes):
- {db}.trips_cleaned (processed table with all trip data)
- {db}.view_fare_by_payment_type (view: revenue by payment type)
- {db}.view_popular_pickup_zones (view: popular pickup zones)
- {db}.view_trip_volume_by_hour (view: trip volume by hour)
- {db}.view_revenue_by_time (view: revenue by hour)
- {db}.view_trip_volume_by_zone (view: trip volume by zone)

Insights Tables (can be queried directly):
- {db}.revenue_by_payment_type (has payment_type - JOIN with {db}.payment_type_lookup)
- {db}.tip_analysis (has payment_type - JOIN with {db}.payment_type_lookup)
- {db}.revenue_by_time (no encoded columns)
- {db}.revenue_by_vendor (has vendorid - JOIN with {db}.vendor_lookup)
- {db}.popular_pickup_zones (has location_id - JOIN with {db}.taxi_zone_lookup)
- {db}.popular_dropoff_zones (has location_id - JOIN with {db}.taxi_zone_lookup)
- {db}.zone_pair_analysis (has pickup_location_id and dropoff_location_id - JOIN with {db}.taxi_zone_lookup)
- {db}.trip_volume_by_zone (has pulocationid - JOIN with {db}.taxi_zone_lookup)
- {db}.trip_duration_by_zone (has pulocationid - JOIN with {db}.taxi_zone_lookup)
- {db}.airport_pickup_zones (has pulocationid - JOIN with {db}.taxi_zone_lookup)
- {db}.airport_dropoff_zones (has dolocationid - JOIN with {db}.taxi_zone_lookup)
- {db}.trip_volume_by_hour (no encoded columns)
- {db}.trip_volume_by_day (no encoded columns)

Lookup Tables (reference data - use for JOINs):
- {db}.payment_type_lookup - Maps payment_type_id to payment_type_name
  Columns: payment_type_id, payment_type_name
- {db}.vendor_lookup - Maps vendor_id to vendor_name
  Columns: vendor_id, vendor_name
- {db}.taxi_zone_lookup - Maps LocationID to Zone, Borough, service_zone
  Columns: LocationID, Borough, Zone, service_zone

Key Metrics:
- Total trips: COUNT(*) of all records
- Revenue: SUM(total_amount)
- Average fare: AVG(fare_amount)
- Utilization: trips per time period

Important Fields:
- vendorid: Taxi vendor identifier (JOIN with {db}.vendor_lookup for names)
- tpep_pickup_datetime: Trip start time
- tpep_dropoff_datetime: Trip end time
- trip_distance: Distance in miles
- fare_amount: Base fare from meter
- tip_amount: Tip amount (credit card only)
- total_amount: Final charge including all fees
- payment_type: Payment method code (JOIN with {db}.payment_type_lookup for names)
- congestion_surcharge: Congestion pricing fee

CRITICAL: Always use JOIN with lookup tables to decode encoded columns to readable names.
Never return raw numeric codes. Always JOIN with the appropriate lookup table.

Payment Type Decoding:
- Tables with payment_type column: {db}.revenue_by_payment_type, {db}.tip_analysis, {db}.trips_cleaned
- JOIN pattern (ALWAYS use fully qualified table names):
  SELECT pt.payment_type_name, r.trip_count, r.avg_fare
  FROM {db}.revenue_by_payment_type r
  JOIN {db}.payment_type_lookup pt ON r.payment_type = pt.payment_type_id

Vendor ID Decoding:
- Tables with vendorid column: {db}.revenue_by_vendor, {db}.trips_cleaned
- JOIN pattern (ALWAYS use fully qualified table names):
  SELECT v.vendor_name, r.trip_count, r.total_revenue
  FROM {db}.revenue_by_vendor r
  JOIN {db}.vendor_lookup v ON r.vendorid = v.vendor_id

Location Zone Decoding:
- Tables with location columns: {db}.popular_pickup_zones, {db}.popular_dropoff_zones, {db}.trip_volume_by_zone, etc.
- Location columns: location_id, pulocationid, dolocationid, pickup_location_id, dropoff_location_id
- JOIN pattern (ALWAYS use fully qualified table names):
  SELECT tz.Zone, tz.Borough, pz.pickup_count
  FROM {db}.popular_pickup_zones pz
  JOIN {db}.taxi_zone_lookup tz ON pz.location_id = tz.LocationID

Always JOIN with {db}.taxi_zone_lookup to get zone names. Include both location_id and zone name columns in results for clarity.

Business Rules:
- Data is partitioned by pickup_year and pickup_month for performance
- Always filter by year and month partitions when possible
- Always use fully qualified table names with double quotes: {db}.table_name
- Database name contains hyphens, so it MUST be wrapped in double quotes
- Average trip: 3-5 miles, $12-15 fare
- Peak hours: 7-9 AM, 5-7 PM on weekdays
- Trips over 100 miles are likely errors

Location Data:
- pulocationid: Pickup location ID (references taxi zones)
- dolocationid: Dropoff location ID (references taxi zones)
- Location IDs 1-3 are airports (JFK, LGA, EWR)

Views Available:
- {db}.view_fare_by_payment_type: Revenue metrics by payment type (has payment_type - JOIN with {db}.payment_type_lookup)
- {db}.view_popular_pickup_zones: Top pickup zones with metrics (has location_id - JOIN with {db}.taxi_zone_lookup)
- {db}.view_trip_volume_by_hour: Trip volume by hour of day
- {db}.view_revenue_by_time: Revenue metrics by hour
- {db}.view_trip_volume_by_zone: Trip volume by pickup zone (has location_id - JOIN with {db}.taxi_zone_lookup)

Important: When querying tables/views with payment_type, vendorid, or location_id columns, ALWAYS JOIN with the appropriate lookup table using fully qualified names ({db}.lookup_table_name) to decode them.
"""
        try:
            # Use legacy train() method for documentation
            self.vanna.train(documentation=documentation)
            print("  Added business context documentation")
        except Exception as e:
            print(f"Error adding documentation: {e}")
            import traceback
            traceback.print_exc()
    
    def _train_sample_queries(self):
        """Train with sample query patterns"""
        db = f'"{Config.ATHENA_DATABASE}"'
        samples = [
            {
                "question": "What was the total number of trips in January 2025?",
                "sql": f"""
                    SELECT COUNT(*) as total_trips
                    FROM {db}.trips_cleaned
                    WHERE pickup_year = '2025' AND pickup_month = '1'
                """
            },
            {
                "question": "Show me average fare by payment type",
                "sql": f"""
                    SELECT 
                        pt.payment_type_name as payment_method,
                        v.avg_fare,
                        v.trip_count
                    FROM {db}.view_fare_by_payment_type v
                    JOIN {db}.payment_type_lookup pt ON v.payment_type = pt.payment_type_id
                    ORDER BY v.trip_count DESC
                """
            },
            {
                "question": "Which pickup zones are most popular?",
                "sql": f"""
                    SELECT 
                        tz.Zone as zone_name,
                        tz.Borough,
                        v.location_id,
                        v.pickup_count,
                        v.avg_fare
                    FROM {db}.view_popular_pickup_zones v
                    JOIN {db}.taxi_zone_lookup tz ON v.location_id = tz.LocationID
                    ORDER BY v.pickup_count DESC
                    LIMIT 10
                """
            },
            {
                "question": "What's the impact of congestion fees on revenue?",
                "sql": f"""
                    SELECT 
                        pt.payment_type_name as payment_method,
                        v.total_revenue,
                        v.avg_fare
                    FROM {db}.view_fare_by_payment_type v
                    JOIN {db}.payment_type_lookup pt ON v.payment_type = pt.payment_type_id
                    ORDER BY v.total_revenue DESC
                """
            },
            {
                "question": "Show me trip volume by hour of day",
                "sql": f"""
                    SELECT 
                        pickup_hour,
                        trip_count,
                        avg_fare
                    FROM {db}.view_trip_volume_by_hour
                    ORDER BY pickup_hour
                """
            },
            {
                "question": "What payment methods are used most frequently?",
                "sql": f"""
                    SELECT 
                        pt.payment_type_name as payment_method,
                        r.trip_count,
                        r.total_revenue
                    FROM {db}.revenue_by_payment_type r
                    JOIN {db}.payment_type_lookup pt ON r.payment_type = pt.payment_type_id
                    ORDER BY r.trip_count DESC
                """
            },
            {
                "question": "Compare revenue by payment type",
                "sql": f"""
                    SELECT 
                        pt.payment_type_name as payment_method,
                        v.avg_fare,
                        v.avg_tip,
                        v.total_revenue
                    FROM {db}.view_fare_by_payment_type v
                    JOIN {db}.payment_type_lookup pt ON v.payment_type = pt.payment_type_id
                    ORDER BY v.total_revenue DESC
                """
            },
            {
                "question": "Which payment type has the highest average fare?",
                "sql": f"""
                    SELECT 
                        pt.payment_type_name as payment_method,
                        v.avg_fare,
                        v.trip_count
                    FROM {db}.view_fare_by_payment_type v
                    JOIN {db}.payment_type_lookup pt ON v.payment_type = pt.payment_type_id
                    ORDER BY v.avg_fare DESC
                    LIMIT 1
                """
            }
        ]
        
        sample_count = 0
        for sample in samples:
            try:
                # Use legacy train() method for question-SQL pairs
                # Skip if it requires email/remote storage - DDLs are more important
                try:
                    self.vanna.train(question=sample['question'], sql=sample['sql'])
                    sample_count += 1
                except Exception as e:
                    # If training fails (e.g., requires remote storage), skip it
                    # DDLs and documentation are more critical
                    if 'email' in str(e).lower() or 'remote' in str(e).lower():
                        print(f"  ⚠️  Skipping sample query (requires remote storage): {sample['question'][:50]}...")
                        continue
                    else:
                        raise
            except Exception as e:
                print(f"Error adding sample query: {e}")
                # Continue with other samples
                continue
        
        if sample_count > 0:
            print(f"  Added {sample_count} sample queries")

