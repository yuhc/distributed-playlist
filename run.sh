#!/bin/bash
seq 0 4|xargs -i -n 1 -P 5 ./obj/process {}
