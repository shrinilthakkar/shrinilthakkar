# Always prefer setuptools over distutils
import json
import os
from codecs import open

import pip
from setuptools import setup, find_packages
from setuptools.command.install import install

try:
    ENV = open('/etc/moe_package_env').read().strip() or 'prod'
except IOError:
    ENV = os.environ.get('MOE_PACKAGE_ENV') or 'prod'

package_name = 'commons'
package_installed_name = package_name + '-' + ENV

# Get the long description from the README file
with open('README.rst', encoding='utf-8') as f:
    long_description = f.read()

with open('VERSION', encoding='utf-8') as f:
    package_version = f.read()

if os.path.exists('fury.json'):
    with open('fury.json', encoding='utf-8') as f:
        fury_dependencies = (json.load(f)).get('dependencies', [])
else:
    fury_dependencies = []

env_fury_dependencies = map(lambda x: x + '-' + ENV, fury_dependencies)


test_dependencies = [
    'nose>=1.3.7',
    'mock>=2.0.0',
]


# Section to download configuration files from S3 post package install

class PackageInstall(install):
    def run(self):
        install.run(self)
        try:
            from moengage.package.post_install import PostInstall
            PostInstall(install_obj=self).run()
        except Exception, e:
            print "Exception while installing package - %r" % e
            pip.main(['uninstall', '--yes', package_installed_name])
            raise

# Post Install Section Ended

print "**************************************************"
print "Installing package: " + package_installed_name + " version: " + package_version
print "**************************************************"

pip.main(['install', '-r', 'requirements.txt'])


setup(
    name=package_installed_name,

    namespace_packages=['moengage'],

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version=package_version,

    description='Project containing common tools to be used across all moengage projects',
    long_description=long_description,

    # The project's main homepage.
    url='https://github.com/moengage/commons',

    # Author details
    author='Akshay Goel',
    author_email='akshay@moengage.com',

    # Choose your license
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Commons',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2.7'
    ],

    # What does your project relate to?
    keywords='moengage commons loggers',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'scripts', 'build', 'dist']),
    package_data={
        'moengage': ['workers/config/*.conf*', 'workers/config/*.ini*', 'workers/config/worker_*',
                     'workers/config/beat_*'],
        '': ['VERSION']
    },
    cmdclass={
        'install': PackageInstall
    },

    # Alternatively, if you want to distribute just a my_module.py, uncomment
    # this:
    #   py_modules=["my_module"],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=env_fury_dependencies,

    tests_require=test_dependencies,

    test_suite="tests",

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
    extras_require={
        'test': ['coverage']
    },
    entry_points={
        'console_scripts': [
            'moengage_worker = moengage.workers.manager:main',
            'moengage_worker_beat = moengage.workers.manager:beat_main',
            'moeconfgen = moengage.config_manager.config_generator:main'
        ]
    }
)
