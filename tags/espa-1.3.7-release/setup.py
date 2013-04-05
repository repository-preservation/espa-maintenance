import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "espa",
    version = "1.3.7",
    author = "David V. Hill",
    author_email = "davehill75@gmail.com",
    description = ("A modular Chain of Command based architecture to "
                                   "support orchestration of science " 					   "executables"),
    license = "NASA Open Source Agreement",
    keywords = "python chain of command science processing",
    url = "http://espa.sourceforge.net",
    packages=['espa',],
    long_description=read('README'),
    classifiers=[
        "Development Status :: Beta",
        "Topic :: Science Processing and Orchestration",
        "License :: OSI Approved :: NASA Open Source Agreement",
    ],
 )
