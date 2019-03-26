import setuptools


def readme():
    with open('README.md') as f:
        return f.read()


setuptools.setup(
    name="pk_kafka",
    version="1.0.2",
    author="Francesco Bruni",
    author_email="Francesco Bruni",
    description="PK Kafka module",
    long_description=readme(),
    packages=setuptools.find_packages(),
    url="",
    install_requires=['confluent-kafka'],
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
