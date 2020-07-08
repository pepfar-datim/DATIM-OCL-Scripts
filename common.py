APP_VERSION = '0.1.0'
OCL_ENVIRONMENTS = {
    'qa': 'https://api.qa.openconceptlab.org',
    'staging': 'https://api.staging.openconceptlab.org',
    'production': 'https://api.openconceptlab.org',
    'demo': 'https://api.demo.openconceptlab.org',
}

# Argument parser validation functions
def ocl_environment(string):
    """ Return OCL environment URL for the specified enviroment key"""
    if string not in OCL_ENVIRONMENTS:
        raise argparse.ArgumentTypeError('Argument "env" must be %s' % (
            ', '.join(OCL_ENVIRONMENTS.keys())))
    return OCL_ENVIRONMENTS[string]
