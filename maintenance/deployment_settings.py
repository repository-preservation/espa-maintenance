tiers = ['webapp', 'maintenance']

environments = {
    'local': {
        'user': 'test',
        'tiers': {
            'webapp': 'localhost',
            'maintenance': 'localhost'
        }
    }
}
