# Contributing to Zaino

Welcome to Zaino and thank you for your interest in contributing! We look forward to your contribution to this important part of the Zcash mainnet and testing ecosystem. This guide will help you get started.

## Table of Contents
- [Getting Started](#getting-started)
- [How to Contribute](#how-to-contribute)
- [Bug Reports](#bug-reports)
- [Feature Requests](#feature-requests)
- [Development Workflow](#development-workflow)
- [Local Testing](#local-testing)
- [Communication Channels](#communication-channels)
- [Software Philosophy](#software-philosophy)

## Getting Started
To get started, please see our [use_cases.md document](./docs/use_cases.md) where you can find instructions for use and example use cases.

## How to Contribute
We welcome and appreciate contributions in the form of code, documentation, bug reports and feature requests. We also generally enjoy feedback and outreach efforts.

Bug reports and feature requests can best be opened as issues on this GitHub repo. Especially for bug reports, any details you can offer will help us understand the issue better. Such details include versions or commits used in exposing the bug, what operating system is being used, and so on.

Code and documentation are very helpful and the lifeblood of Free Software. To merge in code to this repo, one will have to have a GitHub account, and the ability to cryptographically verify commits against this identity. 
The best way is using a GPG signature. See [this document about commit signature verification.](https://docs.github.com/en/authentication/managing-commit-signature-verification/about-commit-signature-verification)
Code, being Rust, should be formatted using `rustfmt` and applying the `clippy` suggestion.
Documentation should be clear and accurate to the latest commit on `dev`.
These contributions must be GitHube pull requests opened _from a personal fork_ of the project, _to this repo, zingolabs/zaino_. Generally pull requests will be against `dev`, the development branch.

## Software Philosophy
We believe in the power of Free and Open Source Software (FOSS) as the best path for individual and social freedom in computing.

Very broadly, Free Software provides a clear path to make software benefit its users. That is, Free Software  has the possibility to be used it like a traditional tool, extending the user's capabilities, unlike closed source software which constrains usage, visability and adaptability of the user while providing some function.

In more detail, the Free Software Foundation states FOSS allows:
The freedom to run a program, for any purpose;
The freedom to study how a program works and adapt it to a person’s needs. Access to the source code is a precondition for this;
The freedom to redistribute copies so that you can help your neighbour; and
The freedom to improve a program and release your improvements to the public, so that the whole community benefits. Access to the source code is a precondition for this.

Developing from this philosophical perspective has several practical advantages:
Reduced duplication of effort
Building upon the work of others
Better quality control
Reduced maintenance costs
 
To read more see: https://en.wikibooks.org/wiki/FOSS_A_General_Introduction/Preface

