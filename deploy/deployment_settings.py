tiers = ['webapp', 'maintenance', 'production']

environments = {
    'local': {
        'user': 'test',
        'tiers': {
            'webapp': 'localhost',
            'maintenance': 'localhost',
            'production': 'localhost'
        }
    }
}