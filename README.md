# hotplumb

A Clojure (and ClojureScript) library to ease building, inspecting and
modifying topologies of intercommunicating goroutines.

The name is a portmanteau of "hotplug" and "plumbing": Hotplumb can re-plumb a
live graph with data streaming through it at runtime. Hotplumb also has support
for programmatically building graphs using a provided factory function -- and
*rebuilding* an existing graph's generated nodes by modifying this function
(Prismatic's "plumbing" library is liable to be useful here).

Among the interesting properties:

- When a live reconfiguration is underway, no piece of data will be sent
  through both pre- and post-reconfiguration nodes. This is accomplished by
  sending reconfiguration messages through the same channels used for data,
  implicitly creating a serialization order.
- Nodes have visibility into the sinks reading from them. No clients interested
  in your data? Stop generating it!
- Each piece of data knows where it's been -- a critical replacement for
  stack-trace information not available in traditional asynchronous frameworks.

## Concepts

A Hotplumb topology is driven by sinks: nodes which act as endpoints. Sinks are
described with lists of which sources they draw from; if a sink wants to read
from a source which doesn't exist, a factory function may be called to try to
bring that source into existance.

All nodes in a Hotplumb topology are keyed by Clojure data structures. These
keys can have meaning; for instance, a node with the key

    {:subscriptions #{"source/foo" "source/bar"}, :rollup-window 10000}

...might always be generated to have two parents:

    {:subscription "source/foo" :rollup-window 10000}
    {:subscription "source/bar" :rollup-window 10000}

...each of those having two parents in turn, ie.

    {:subscription "source/foo"} {:timeout 10000}

...and the responsibility for rolling up content generated thereby into 10-second summaries.

## Usage

FIXME

## History and Future

- 0.0.1: First proof-of-concept (YOU ARE HERE!)
- 0.1.0: Target for first production-ready version in terms of runtime robustness.
- 1.0.0: First version where API stability and backwards compatibility are promised.

### Known Bugs and Pending Features

- CRITICAL: Reflow does not work for errored-out goroutines.
- CRITICAL: Safe reflow ordering not guaranteed with topologies wherein a node may have multiple sources.
- Optimization: do not rerun ChangeRequests on nodes where previously ack'd, or propagate from same.
- Optimization: build changelists during simple edits to allow faster application
- Feature: timeout support
- Think about whether this can be decomplected into smaller components
- Think about dropping efforts at independence from core.async

## License

Copyright © 2014 ThreatGRID, Inc.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
