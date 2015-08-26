from distutils.core import setup

setup(
    name='emrclient',
    packages=['emrclient'],
    version='0.1',
    description='This is created to view and kill applications/jobs running on yarn inside Amazon EMR or any other remote location. Currently the amazon api does not include stopping jobs. Also supports deploying steps',
    author='Mark Kelly',
    author_email='mkelly28@tcd.ie',
    # url='https://github.com/zalando/yarncli',
    # download_url='https://github.com/zalando/yarncli/tarball/0.1',
    keywords=['emr', 'aws', 'yarn', 'kill', 'view', 'client', 'spark'],
    classifiers=[],
    entry_points={'console_scripts': 'emrclient = emrclient.cli:main'},
    install_requires=['clickclick','tabulate'],
    license='Apache License 2.0'
)
