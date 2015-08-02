# Clustercluck #

Failure-Tolerant Distributed Clucking Goodness

This project is a simple joke meant to annoy my co-workers. Each node in
the cluster is designated as a 'hen'. These hens collectively implement
RAFT, allowing them to reach consensus on commands to execute.

Commmands:

* cluck - write 'Cluck!' to the system log.
* hatch - add a new hen to the cluster.
* pluck - remove a hen from the cluster.
* roost - shut down the cluster.

I do not expect this to work anywhere except for my company's test machines.

Thanks and Gig 'Em!

## License ##

I don't suppose this is legally binding, but eh. MIT license for glory.

```
Copyright (c) 2015 Takmo

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```
