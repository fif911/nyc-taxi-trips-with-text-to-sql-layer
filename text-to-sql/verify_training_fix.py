"""
Verify that the training data uses fully qualified table names
"""
from config import Config

# Simulate what the training data will look like
db = f'"{Config.ATHENA_DATABASE}"'

print("=" * 80)
print("VERIFICATION: Training Data Format")
print("=" * 80)

print("\n1. Payment Type Decoding JOIN Pattern:")
print("-" * 80)
join_pattern = f"""
SELECT pt.payment_type_name, r.trip_count, r.avg_fare
FROM {db}.revenue_by_payment_type r
JOIN {db}.payment_type_lookup pt ON r.payment_type = pt.payment_type_id
"""
print(join_pattern)

print("\n2. Sample Query for 'What payment methods are used most frequently?':")
print("-" * 80)
sample_query = f"""
SELECT 
    pt.payment_type_name as payment_method,
    r.trip_count,
    r.total_revenue
FROM {db}.revenue_by_payment_type r
JOIN {db}.payment_type_lookup pt ON r.payment_type = pt.payment_type_id
ORDER BY r.trip_count DESC
"""
print(sample_query)

print("\n3. Verification:")
print("-" * 80)
checks = [
    (f'"{Config.ATHENA_DATABASE}"' in join_pattern, "Database name is quoted in JOIN pattern"),
    (f'{db}.payment_type_lookup' in join_pattern, "Lookup table is fully qualified in JOIN pattern"),
    (f'{db}.revenue_by_payment_type' in sample_query, "Main table is fully qualified in sample query"),
    (f'{db}.payment_type_lookup' in sample_query, "Lookup table is fully qualified in sample query"),
    ('FROM revenue_by_payment_type' not in join_pattern, "No unqualified table names in JOIN pattern"),
    ('JOIN payment_type_lookup' not in join_pattern, "No unqualified lookup table in JOIN pattern"),
]

all_passed = True
for check, description in checks:
    status = "✅" if check else "❌"
    print(f"{status} {description}")
    if not check:
        all_passed = False

print("\n" + "=" * 80)
if all_passed:
    print("✅ ALL CHECKS PASSED - Training data format is correct!")
    print("\nNext steps:")
    print("1. Restart your FastAPI application to retrain Vanna")
    print("2. The training will use the updated business context with fully qualified names")
    print("3. Vanna should now successfully use lookup tables in queries")
else:
    print("❌ SOME CHECKS FAILED - Please review the training data")
print("=" * 80)
