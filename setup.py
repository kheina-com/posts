try :
	from re import _pattern_type as Pattern
	from re import compile as re_compile

except ImportError :
	from re import Pattern, compile as re_compile

from os import listdir

from setuptools import find_packages, setup

from fuzzly_posts import __version__


req_regex: Pattern = re_compile(r'^requirements-(\w+).txt$')

# pyx_files: List[str] = []
# for root, directories, files in os.walk('.'):
# 	for f in fnmatch.filter(files, '*.pyx'):
# 		pyx_files.append(os.path.join(root, f))
# pyx_extensions = cythonize(pyx_files)


setup(
	name='fuzzly-posts',
	version=__version__,
	description='posts library for fuzz.ly',
	long_description=open('readme.md').read(),
	long_description_content_type='text/markdown',
	author='kheina',
	url='https://github.com/kheina-com/posts',
	packages=find_packages(exclude=['tests']),
	install_requires=list(filter(None, map(str.strip, open('requirements.txt').read().split()))),
	python_requires='>=3.9.*',
	license='Mozilla Public License 2.0',
	extras_require=dict(map(lambda x : (x[1], open(x[0]).read().split()), filter(None, map(req_regex.match, listdir())))),
	# ext_modules=cythonize(pyx_files),
)