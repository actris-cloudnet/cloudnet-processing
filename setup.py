from setuptools import setup, find_packages


setup(name='data_processing',
      description='Software for running Cloudnet processing.',
      author='Finnish Meteorological Institute',
      license='MIT License',
      setup_requires=['wheel'],
      install_requires=['cloudnetpy>=1.2.1',
                        'tqdm',
                        'pytest',
                        'pyyaml',
                        'requests_mock',
                        'fastapi',
                        'uvicorn',
                        'python-multipart',
                        ],
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
