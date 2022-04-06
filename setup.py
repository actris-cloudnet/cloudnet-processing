from setuptools import find_packages, setup

version: dict = {}
with open("src/cloudnet_processing/version.py", encoding="utf-8") as f:
    exec(f.read(), version)  # pylint: disable=W0122

setup(
    name="cloudnet_processing",
    description="Software for running Cloudnet processing.",
    version=version["__version__"],
    author="Finnish Meteorological Institute",
    license="MIT License",
    setup_requires=["wheel"],
    install_requires=[
        "cloudnetpy_qc>=0.1.1",
        "cloudnetpy>=1.31.2",
        "cloudnetme>=0.1.5",
        "requests",
    ],
    extras_require={
        "test": [
            "pytest",
            "pylint",
            "mypy",
            "requests_mock",
            "types-requests",
            "types-pytz",
            "types-urllib3",
        ],
        "dev": ["pre-commit"],
    },
    include_package_data=True,
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering",
    ],
)
