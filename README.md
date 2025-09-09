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

:file_folder: _test_cache
    :file_folder: <test_notebook_name>
        :file_folder: type= notebook|job (It's a test type)
            :file_folder: tasks
                :file_folder: <test_function_name>
                    :ledger: <task_notebooks>
            :ledger: <test_function_name>

:file_folder: <test_folder>
    :ledger: <test_notebook>
        :file_folder: <sub_folders>
            :ledger: <test_notebook>
