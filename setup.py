from setuptools import setup, find_packages


setup(name='operational-processing',
      description='Software for running operational CloudnetPy processing.',
      author='Finnish Meteorological Institute',
      license='MIT License',
      install_requires=['cloudnetpy @ git+https://github.com/actris-cloudnet/cloudnetpy',
                        'watchdog',
                        'tqdm',
                        'pytest',
                        'pyyaml',
                        'pytest',
                        'requests'],
      include_package_data=True,
      package_dir={"": "src"},
      packages=find_packages(where="src"),
      python_requires='>=3.6',
      classifiers=[
          "Programming Language :: Python :: 3.6",
          "License :: OSI Approved :: MIT License",
          "Intended Audience :: Science/Research",
          "Topic :: Scientific/Engineering",
      ],
      )
