"""
FastQ是一款由 [德波量化](http://www.dealbot.cn) 开源的基于消息队列的多进程分布式任务调度器，原应用于Dealbot量化策略高并发回测。
目前版本暂时仅支持Python语言，消息队列仅支持Redis。
"""
import os
from setuptools import find_packages, setup


def get_requirements():
    basedir = os.path.dirname(__file__)
    try:
        with open(os.path.join(basedir, 'requirements.txt')) as f:
            return f.readlines()
    except FileNotFoundError:
        raise RuntimeError('No requirements info found.')


setup(
    name='fasttq',
    version=__import__('fasttq').__version__,
    url='https://github.com/wakeblade/fasttq',
    license='BSD',
    author='Wakeblade',
    author_email='2390245@qq.com',
    description='FastTQ是一款由 [德波量化](http://www.dealbot.cn) 开源的基于消息队列的多进程分布式任务调度器',
    long_description=__doc__,
    packages=find_packages(exclude=['examples', '*.py']),
    package_data={"fasttq": ["py.typed"]},
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=get_requirements(),
    python_requires='>=3.6',
    entry_points={},
    classifiers=[
        # As from http://pypi.python.org/pypi?%3Aaction=list_classifiers
        # 'Development Status :: 1 - Planning',
        # 'Development Status :: 2 - Pre-Alpha',
        # 'Development Status :: 3 - Alpha',
        # 'Development Status :: 4 - Beta',
        'Development Status :: 5 - Production/Stable',
        # 'Development Status :: 6 - Mature',
        # 'Development Status :: 7 - Inactive',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Systems Administration',
        'Topic :: System :: Monitoring',
    ],
)
