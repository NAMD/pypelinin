#!/bin/bash

checkout_and_install() {
    cd pypelinin-repo
    git checkout $2
    cd ..
    cp -r pypelinin-repo pypelinin-$1
    virtualenv venv-pypelinin-$1
    source venv-pypelinin-$1/bin/activate
    pip install -r pypelinin-repo/requirements/development.txt
    cd pypelinin-$1
    python setup.py install
    cd ..
    deactivate
    rm -rf pypelinin-$1
}

git clone git://github.com/turicas/pypelinin.git pypelinin-repo
checkout_and_install 0.1.0 bcd963432703b7f92ff6a98924003009ed6c079c
checkout_and_install fixed ac52b4e13e96bb76bdf48fb644a98ea9d0a46514
rm -rf pypelinin-repo
