# kung-fu-pipelines

[![Build Status](https://travis-ci.org/smk508/kung-fu-pipelines.svg?branch=master)](https://travis-ci.org/smk508/kung-fu-pipelines)
[![PyPI version](https://badge.fury.io/py/kung-fu-pipelines.svg)](https://badge.fury.io/py/kung-fu-pipelines)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/smk508/kung-fu-pipelines/branch/master/graph/badge.svg)](https://codecov.io/gh/smk508/kung-fu-pipelines)
[![Documentation Status](https://readthedocs.org/projects/kung-fu-pipelines/badge/?version=latest)](https://kung-fu-pipelines.readthedocs.io/en/latest/?badge=latest)

Argo is a great system for orchestrating data science workflows, but fidgeting
around with yaml files to write a pipeline can be frustrating. Although there
are libraries like [kfp] and various argo SDKs which let you specify workflows
programmatically, there's still a lot of boilerplate. Running data science workloads in the cloud should be easier. 


kung-fu-pipelines abstracts away the boilerplate so you can focus on the logic
of your workflow steps.

*This library is a work in progress. Feedback is welcome
:)*

# Getting Started

This library contains two main classes. A `Step` represents a single step in a
workflow. It contains information about the logic to be performed by that `Step`
along with metadata such as the parameters the `Step` expects and a description.
A `Step` can automatically generate the appropriate YAML for performing that

A `Workflow` is a template for organizing `Step` objects together in a certain
way. Many types of pipelines follow the same general structure, so you can
create a `Workflow` which encapsulates this structure and then instantiate it
with the `Step`s relevant to your specific pipeline and then generate the Argo
YAML automatically.
For example, a machine learning `Workflow` might have the following steps:

1) Construct reference dataset
2) Train/test split
3) Preprocess training data
4) Train model
5) Evaluate model on test set

You could create a `Workflow` which has these steps, or whatever variation you
need, and then for each of your experiments just use that `Workflow` while
swapping in the appropriate `Step` objects.

# Installation 

    pip install kung-fu-pipelines

# Concepts

## Step

## CLI

## Workflow


# Contributing 

Pull requests, issues, questions, and comments are welcome. You can also reach
me directly at skhan8@mail.einstein.yu.edu.