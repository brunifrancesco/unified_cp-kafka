import setuptools


def readme():
    with open('README.md') as f:
        return f.read()

def requirements():
    with open('requirements.txt') as f:
        return f.read().splitlines()

setuptools.setup(
    name="pk_kafka",
    version="1.2.0",
    author="Francesco Bruni",
    author_email="Francesco Bruni",
    description="PK Kafka module",
    long_description=readme(),
    packages=setuptools.find_packages(),
    url="",
    install_requires=requirements(),
    include_package_data=True,
    package_data={
        '': ['README.md']
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
