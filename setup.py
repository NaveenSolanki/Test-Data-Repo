from setuptools import setup, find_packages

setup(
    name="algo-trading-system",
    version="1.0.0",
    packages=find_packages(),  # Automatically discover and include packages
    install_requires=[],
    entry_points={
        'console_scripts': [
            'algo-trading-system = src.trading_algo.main:trade_algo_structure.py',  # Customize the entry point
        ],
    },
)
