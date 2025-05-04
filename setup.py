from setuptools import setup, Extension
from pybind11.setup_helpers import Pybind11Extension, build_ext

# Define extensions
ext_modules = [
    Pybind11Extension(
        "cpp_mapper",
        ["mapper.cpp"],
        include_dirs=[],
        language="c++",
        extra_compile_args=["-std=c++17"],
    ),
    Pybind11Extension(
        "cpp_reducer",
        ["reducer.cpp"],
        include_dirs=[],
        language="c++",
        extra_compile_args=["-std=c++17"],
    ),
]

setup(
    name="mapreduce_cpp",
    version="0.1",
    description="C++ performance-critical components for MapReduce",
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
    zip_safe=False,
    python_requires=">=3.6",
    install_requires=["pybind11>=2.6.0"],
)
