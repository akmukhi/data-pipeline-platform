"""Setup configuration for data-pipeline-platform."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="data-pipeline-platform",
    version="0.1.0",
    author="Data Pipeline Team",
    description="A scalable data pipeline platform for ETL operations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/data-pipeline-platform",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.11",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "pipeline-cli=cli.main:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)

