from setuptools import setup, find_packages

setup(
    name='fastTGA',
    version='0.2.1',
    packages=find_packages(),
    install_requires=[
        'PyQt6',
        'gspread',
        'polars',
    ],
    entry_points={
        'console_scripts': [
            'fastTGA=fastTGA.main:main',
        ],
    },
    author='Manuel Leuchtenmüller',
    author_email='manuel.leuchtenmueller@hydrogenreductionlab.com',
    description='A package for TGA data processing and management',
    url='https://github.com/yourusername/fastTGA',
)