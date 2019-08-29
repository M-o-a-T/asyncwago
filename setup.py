from setuptools import setup, find_packages

LONG_DESC = open("README.rst", encoding="utf-8").read()

setup(
    name="wago",
    use_scm_version={"version_scheme": "guess-next-dev", "local_scheme": "dirty-tag"},
    setup_requires=["setuptools_scm"],
    description="Access I/O on Wago controllers",
    url="http://github.com/M-o-a-T/wago",
    long_description=LONG_DESC,
    author="Matthias Urlichs",
    author_email="matthias@urlichs.de",
    license="GPLv3 or later",
    packages=find_packages(),
    install_requires=[
        "anyio >= 1.0",
    ],
    keywords=[
        "iot",
    ],
    python_requires=">=3.6",
    classifiers=[
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Framework :: Trio",
    ],
)
