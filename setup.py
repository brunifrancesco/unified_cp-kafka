import setuptools


def readme():
    with open('README.md') as f:
        return f.read()


def dependencies():
    with open('requirements.txt') as f:
        return f.readlines()


setuptools.setup(
    name="pk_kafka_module",
    version="1.0.0",
    author="Francesco Bruni",
    author_email="Francesco Bruni",
    description="PK Kafka module",
    long_description=readme(),
    packages=setuptools.find_packages(),
    url="",
    install_requires=dependencies(),
    include_package_data=True,
    package_data={
        '': ['README.md', 'requirements.txt']
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
