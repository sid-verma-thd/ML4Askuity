
from distutils.core import setup
setup(name='teamUtils',
      version='1.0.2',
      py_modules=['teamUtils'],
      install_requires=[
          'pydata_google_auth>=0.3.0',
          'pandas>=1.1.3',
          'numpy>=1.19.1',
          'google.cloud>=0.34.0',
          'google.cloud.bigquery>=2.1.0',
          'google.cloud.bigquery_storage>=2.4.0',
          'google.cloud.storage>=1.28.1',
          'pyarrow>=2.0.0',
          'pandas-gbq>=0.13.1'
      ]
)
