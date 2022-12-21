import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="watobs",
    version="0.2.dev2",
    install_requires=["numpy", "pandas", "requests", "matplotlib"],
    extras_require={
        "dev": [
            "pytest",
            "flake8",
            "black",
            "sphinx",
            "myst-parser",
            "sphinx-book-theme",
        ],
        "test": ["pytest", "mikeio"],
        "notebooks": [
            "nbformat",
            "nbconvert",
            "jupyter",
            "folium",
            "rtree",
            "mapclassify",
            "geopandas",
            "xarray",
            "netcdf4",
            "NDBC",
        ],
    },
    author="Jesper Sandvig Mariegaard",
    author_email="jem@dhigroup.com",
    description="Access water observation data",
    license="MIT",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DHI/watobs",
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering",
    ],
)
