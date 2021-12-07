from setuptools import setup, find_packages

setup(
    name='python-async-sqs-consumer',
    version='0.0.1',
    url='https://github.com/tonisantes/python-async-sqs-consumer',
    license='',
    author='Toni Santes',
    author_email='tonisantes@gmail.com',
    description='Python client for consuming messages asynchronously from AWS Simple Queue Service (SQS)',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'aiobotocore==2.0.1',
        'asyncio-pool==0.5.2',
    ]
)