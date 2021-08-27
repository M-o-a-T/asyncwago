from setuptools import setup, find_packages

LONG_DESC = open("README.rst", encoding="utf-8").read()

setup(
    name="asyncwago",
    use_scm_version={"version_scheme": "guess-next-dev", "local_scheme": "dirty-tag"},
    setup_requires=["setuptools_scm"],
    description="Access I/O on Wago controllers",
    url="http://github.com/M-o-a-T/asyncwago",
    long_description=LONG_DESC,
    author="Matthias Urlichs",
    author_email="matthias@urlichs.de",
    license="GPLv3 or later",
    packages=find_packages(),
    install_requires=["anyio >= 3.2", 'importlib-metadata ~= 1.0 ; python_version < "3.8"'],
    keywords=["iot"],
    python_requires=">=3.6",
    classifiers=[
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Framework :: Trio",
    ],
)
