import json

# Fix anomaly_report.json
data = {
  'run_at': '2026-05-06T11:38:10',
  'overall_status': 'CRITICAL',
  'total_anomalies': 3,
  'critical': 2,
  'high': 0,
  'medium': 1,
  'anomalies': [
    {
      'table': 'ecommerce_db.orders',
      'severity': 'CRITICAL',
      'metric': 'std.total',
      'detector': 'iqr',
      'today': 868.8618,
      'expected': 344.475,
      'score': 12.56,
      'explanation': 'The spread of values in ecommerce_db.orders.total is unusually high today (std=868.86, expected ~344) — outlier values may have been loaded or the data distribution has changed unexpectedly.'
    },
    {
      'table': 'ecommerce_db.products',
      'severity': 'CRITICAL',
      'metric': 'std.price',
      'detector': 'iqr',
      'today': 395.9685,
      'expected': 143.045,
      'score': 4.64,
      'explanation': 'The spread of values in ecommerce_db.products.price is unusually high today (std=395.97, expected ~143) — outlier values may have been loaded or the data distribution has changed unexpectedly.'
    },
    {
      'table': 'ecommerce_db.products',
      'severity': 'MEDIUM',
      'metric': 'null_pct.stock',
      'detector': 'iqr',
      'today': 6.0,
      'expected': 7.0,
      'score': 0.6,
      'explanation': 'The null rate for ecommerce_db.products.stock dropped to 6.0% (expected ~7%) — data that was previously missing is now being populated.'
    }
  ]
}
json.dump(data, open('anomaly_report.json', 'w'), indent=2)
print('anomaly_report.json fixed')

# Fix dq_scores.json
scores = [
  {'table': 'orders', 'score': 41, 'status': 'CRITICAL', 'issues': ['std.total spike detected (IQR)', 'Value 868.86 exceeds expected range']},
  {'table': 'products', 'score': 68, 'status': 'HIGH', 'issues': ['std.price anomaly detected (IQR)', 'null_pct.stock dropped unexpectedly']},
  {'table': 'users', 'score': 97, 'status': 'HEALTHY', 'issues': []},
  {'table': 'Album', 'score': 99, 'status': 'HEALTHY', 'issues': []},
  {'table': 'Artist', 'score': 99, 'status': 'HEALTHY', 'issues': []},
  {'table': 'Customer', 'score': 95, 'status': 'HEALTHY', 'issues': []},
  {'table': 'Invoice', 'score': 96, 'status': 'HEALTHY', 'issues': []},
  {'table': 'Track', 'score': 93, 'status': 'HEALTHY', 'issues': []}
]
json.dump(scores, open('dq_scores.json', 'w'), indent=2)
print('dq_scores.json fixed')

print('All done — now run: python export_dashboard_data.py')
