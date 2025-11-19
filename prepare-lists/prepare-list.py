# -*- coding: utf-8 -*-
"""Basic implementation of command line tool that receives a list of unnamed arguments"""

import os
from typing import List

import typer
import pandas as pd


# Instantiate the typer library
app = typer.Typer()


# define the function for the command line
@app.command()
def main():
    pass


# ----------------------------------------------------------------
# Main
# ----------------------------------------------------------------
if __name__ == "__main__":
    app()
