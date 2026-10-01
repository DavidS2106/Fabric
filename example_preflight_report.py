"""Example: How to use the ReportTemplateBuilder with Preflight Validation"""

from html_report_template import ReportTemplateBuilder

# Initialize the builder
builder = ReportTemplateBuilder()

# Define preflight validation items
validation_items = [
    {
        'id': 'check_db_connection',
        'label': 'Database Connection',
        'status': 'passed',
        'message': 'Successfully connected to production database'
    },
    {
        'id': 'check_schema_version',
        'label': 'Schema Version',
        'status': 'warning',
        'message': 'Schema version mismatch: expected 5.2, found 5.1'
    },
    {
        'id': 'check_data_integrity',
        'label': 'Data Integrity',
        'status': 'passed',
        'message': 'All data validation checks passed'
    },
    {
        'id': 'check_permissions',
        'label': 'User Permissions',
        'status': 'failed',
        'message': 'Missing required admin permissions'
    },
    {
        'id': 'check_file_access',
        'label': 'File Access',
        'status': 'passed',
        'message': 'All required files are accessible'
    },
]

# Generate preflight validation HTML
preflight_html = builder.get_preflight_validation_html(
    validation_items=validation_items,
    title='Migration Preflight Validation',
    overall_status='warning'
)

# Create main content with preflight validation
main_content = f"""
<div class="page-wrapper active" id="homePage">
    <div class="report-header">
        <h1>Data Migration Report</h1>
        <p class="report-meta">Generated on 2026-08-26 | Report ID: MIG-001</p>
    </div>

    {preflight_html}

    <div class="section-title">Migration Overview</div>
    <p>This report contains the preflight validation results for your data migration process.
       Please review the validation items above and address any warnings or failures before proceeding.</p>

    <div class="summary-grid">
        <div class="summary-card success">
            <h3>Success Rate</h3>
            <div class="value">80%</div>
        </div>
        <div class="summary-card warning">
            <h3>Warnings</h3>
            <div class="value">1</div>
        </div>
        <div class="summary-card danger">
            <h3>Failures</h3>
            <div class="value">1</div>
        </div>
        <div class="summary-card">
            <h3>Total Checks</h3>
            <div class="value">5</div>
        </div>
    </div>

    <div class="section-title">Next Steps</div>
    <ol>
        <li>Review the preflight validation results above</li>
        <li>Address any failed checks before proceeding</li>
        <li>Resolve schema version compatibility issues</li>
        <li>Request necessary permissions if required</li>
        <li>Run validation again to confirm all checks pass</li>
    </ol>
</div>

<div class="page-wrapper" id="detailsPage">
    <div class="report-header">
        <h1>Detailed Validation Results</h1>
        <p class="report-meta">Generated on 2026-08-26</p>
    </div>

    <div class="section-title">Database Connection Details</div>
    <p><strong>Status:</strong> Connected</p>
    <p><strong>Host:</strong> prod-db-01.internal.mastech</p>
    <p><strong>Port:</strong> 5432</p>
    <p><strong>Database:</strong> migration_db</p>

    <div class="section-title">Schema Compatibility Report</div>
    <p><strong>Current Version:</strong> 5.1</p>
    <p><strong>Required Version:</strong> 5.2</p>
    <p><strong>Action:</strong> Please upgrade schema to version 5.2</p>

    <div class="section-title">Permission Matrix</div>
    <p><strong>Current User:</strong> migration_user</p>
    <p><strong>Required Roles:</strong> admin, data_manager</p>
    <p><strong>Action:</strong> Grant admin role to migration_user</p>
</div>
"""

# Define navigation items for sidebar
nav_items = [
    {'id': 'homePage', 'label': 'Overview', 'icon': 'home'},
    {'id': 'detailsPage', 'label': 'Detailed Results', 'icon': 'file'},
]

# Build the complete HTML report
html_report = builder.wrap_content(
    title='Data Migration Preflight Report',
    report_title='Data Migration',
    subtitle='Preflight Validation Report',
    right_content='Report ID: MIG-001 | 2026-08-26',
    agent_name='Migration Agent',
    version='1.0.0',
    nav_items=nav_items,
    home_label='Home',
    home_onclick="showPage('homePage')",
    section_label='SECTIONS',
    main_content=main_content,
)

# Save to HTML file
output_file = 'migration_preflight_report.html'
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(html_report)

print(f"✓ Report generated successfully!")
print(f"✓ File saved: {output_file}")
print(f"\nTo view the report:")
print(f"  1. Open the file in a web browser")
print(f"  2. Or run: python -m http.server 8000")
print(f"  3. Then navigate to: http://localhost:8000/{output_file}")
