# DBX Tester

DBX Tester is easy to use and highly scalable databricks testing framework. It enables us to create tests in Databricks way.

## Contents
    1. Objectives
    2. Installation
    3. Architecture 
    4. Examples
    5. Contributions
    6. Release notes

## Objective
    The usual and in-built testing frameworks are doesn't fit well within databricks. Also, Dependency management is so hard with other frameworks. So, I ended creating this framework.

## Installation

## Architecture
DBX Tester utilize workspace to create and manage tests. The folder structure of the test and test cache will be like mentioned below


📁 Folder Structure

    📁_test_cache/
    ├── 📁<test_notebook_name>/
    │   └── 📁type=notebook|job/
    │       └── 📁tasks/
    │           └──📒 <task_notebooks>
    │       📁<test_function_name>
    |       └──📒 <test_notebooks>
    <test_folder>/
    ├── 📒 <test_notebook>
    │   └── <sub_folders>/
    │       📒 <test_notebook>

##Examples