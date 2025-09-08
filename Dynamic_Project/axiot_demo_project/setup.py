from setuptools import setup, find_packages

setup(
    name="Dynamic-Automation-Microservice",
    version="0.0.1",
    packages=find_packages(),

    # Customize the install_requires list with your project's dependencies
    install_requires=[
        # list your dependencies here, e.g.,
        # 'numpy', 'requests',
        # "flask", "pymongo", "psycopg2-binary"
    ],
    entry_points={
        'console_scripts': [
            '{project_name}_Dynamic_project=backend.main:__name__',  # If you have a main function to run
        ],
    },
    author="Stanley Parmar",
    author_email="Email Ids here",
    description="This python repository helps to do Dynamic_project Setups done like Tenant, User and Role and validation of "
                " it ",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    giturl="https://github.com/STANLEY56S/Dynamic-Automation-Microservice",
    url="https://github.com/STANLEY56S/Dynamic-Automation-Microservice",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.0, <4',
)
