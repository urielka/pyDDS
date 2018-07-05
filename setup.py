from setuptools import setup, find_packages
import os


setup(name='pydds-rti-xml',
      version='0.1.2',
      description='Python wrapper for RTI DDS with XML application support',
      author='Uriel Katz',
      author_email='uriel.katz@gmail.com',
      url='https://github.com/urielka/pydds-rti-xml',
      include_package_data=True,
      packages = find_packages(),
      py_modules=['dds'],
)
