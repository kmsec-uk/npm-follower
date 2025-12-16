# npm feed follower

golang library for following updates to the npm registry via the CouchDB API (replicate.npmjs.com/registry/_changes).

Designed to be simple to setup and start receiving events through a channel:

```go
package main

import (
	"context"
	"log"

	"github.com/kmsec-uk/npm-replicate-follower/couch"
)

func main() {
	ctx := context.Background()
	log.Println("hello from the printer")
	// create the follower
	f := couch.NewFollower()
	// connect and start receiving changes from the channel
	for event := range f.Connect(ctx) {
		// skip error cases
		if err := event.Error; err != nil {
			log.Printf("error polling: %v\n", err)
			continue
		}

		// skip deletions
		if event.Change.Deleted {
			log.Printf("%s: deleted\n", event.Change.ID)
			continue
		}
        log.Printf("%s: updated!\n", event.Change.ID)
    }
}
```

## Getting full documents

The CouchDB _changes API provides an immediate feed of objects with limited information like this:

```json
    {
      "seq": 89797387,
      "id": "pino",
      "changes": [
        {
          "rev": "37-492fb14479b1ae44c0150d53ab2ce6ba"
        }
      ]
    }
```
However, you're probably interested in enriching this by getting more details about the specific package (`id` property).

This library provides easy-access utilities for getting documents via the Follower. See `/couch/packument.go` for details on exposed APIs.

```go
for event := range f.Connect(ctx) {
		// skip error cases
		if err := event.Error; err != nil {
			log.Printf("error polling: %v\n", err)
			continue
		}
		// skip deletions
		if event.Change.Deleted {
			log.Printf("%s: deleted\n", event.Change.ID)
			continue
		}
		// get full packument details.
        // this is equivalent to GETing https://registry.npmjs.com/pino.
        // The result is deserialised into a Packument struct
		p, err := f.GetPackument(ctx, &event.Change)
		if err != nil {
			log.Printf("%s: error getting packument: %v\n", event.Change.ID, err)
            continue
		}
		latest := p.Latest()
		log.Printf("%s: updated - latest version %s was published by npm user %s (%s)", event.Change.ID, latest.Version, latest.NpmUser.Name, latest.NpmUser.Email)
    }

```

Instead of getting the full npm package Packument -- which can be massive depending on the age of the package -- you can also just get the latest version manifest using Follower.GetLatestVersionManifest()

Lower-level functions are exposed for those that want access to the raw body of the request, e.g. for backing up raw JSON documents. These are exposed as Follower.Fetch*.

## Configuring the Follower

The Follower comes with builder-style configuration:

```go
f := couch.NewFollower()
    .WithUserAgent("my-useragent") // your user-agent here
    .WithPollingInterval(10 * time.Second) // interval to GET _changes
    .WithClientTimeout(15 * time.Second) // http.Client timeout.
    .Since(<uint64>) // if you want to connect from a specific sequence
for event := range f.Connect(ctx) {...}
```

## Replication challenges and known issues

The CouchDB _changes API exposes specific change IDs (`_rev` property) that represent the unique revision of the document (npm package).

Unfortunately, it is common for the actual registry CDN to lag behind the _changes feed, likely due to replication lag or cache.

For example, take the following code, which fetches the packument from the registry after receiving a _changes event (this is the [recommended approach](https://github.com/orgs/community/discussions/152515#discussion-8017309) btw):

```go
	for event := range f.Connect(ctx) {
		// skip error cases
		if err := event.Error; err != nil {
			log.Printf("error polling: %v\n", err)
			continue
		}

		// skip deletions
		if event.Change.Deleted {
			log.Printf("%s: deleted\n", event.Change.ID)
			continue
		}

		// get full packument details from the registry
		p, err := f.GetPackument(ctx, &event.Change)
		if err != nil {
			log.Printf("%s: error getting packument: %v\n", event.Change.ID, err)
			continue
		}

		// check that the Packument _rev property is aligned with the _changes feed _rev
		if !event.Change.HasRevision(p.Rev) {
			log.Printf("%s: Packument revision (_rev property) %s not in _changes feed. CouchDB says %s", event.Change.ID, p.Rev, event.Change.Changes[0].Rev)
		}
		log.Printf("%s: updated - latest version %s", event.Change.ID, p.Latest().Version)
	}
```

When reviewing logs, you might find the Packument _rev on the registry lags behind the _changes feed:

```text
2025/12/15 15:42:43 hotelzify-common: Packument revision (_rev property) 124-c1d354e2804655404fc010d28471c001 not in _changes feed. CouchDB says 125-d98f0b63d0af6ce34c553356b28afdce
2025/12/15 15:42:43 hotelzify-common: updated - latest version 1.1.35 // inaccurate!
2025/12/15 15:42:43 @datagrok/tutorials: Packument revision (_rev property) 80-49a99b90f4d400fb2b2660635a0de432 not in _changes feed. CouchDB says 81-6215048b61d62a33f8ea12a461a962c6
2025/12/15 15:42:43 @datagrok/tutorials: updated - latest version 1.10.5 // inaccurate!
2025/12/15 15:42:43 @grafana/runtime: Packument revision (_rev property) 13899-89acda9c5025247e3bb970e1de2d45fb not in _changes feed. CouchDB says 13900-464c9ae7c8696d905ca065a48d02c38e
```

Effectively, we are *too early* to catch the latest revision.

Without implementing some kind of delay or retry (which comes with their own problems -- namely being too *late* for a _rev or hammering the registry repeatedly), we end up with mismatching documents and effectively an out-of-date replicant. It is *not possible to query the registry for a specific _rev*, making this problem even more stark.

A simple solution is to simply wait for replication and cache update by setting a timer. The full example below (as shown in cmd/printer/main.go) simply waits for 10 seconds, which seems to be long enough for the registry to catch up and short enough that no subsequent updates wipe this history:

```go
func main() {
	ctx := context.Background()
	log.Println("hello from the printer")
	// create the follower
	f := couch.NewFollower().WithPollingInterval(5 * time.Second)
	// connect and start receiving changes from the channel

	var wg sync.WaitGroup
	for event := range f.Connect(ctx) {
		// skip error cases
		if err := event.Error; err != nil {
			log.Printf("error polling: %v\n", err)
			continue
		}

		// skip deletions
		if event.Change.Deleted {
			log.Printf("%s: deleted\n", event.Change.ID)
			continue
		}
		// start goroutine
		wg.Go(func() {
			select {

			case <-time.After(10 * time.Second):
				// wait 10 seconds before getting packument
			case <-ctx.Done():
				return
			}
			// get full packument details
			p, err := f.GetPackument(ctx, &event.Change)
			if err != nil {
				log.Printf("%s: error getting packument: %v\n", event.Change.ID, err)
				return
			}

			// check that the Packument _rev property is aligned with the _changes feed _rev
			if !event.Change.HasRevision(p.Rev) {
				log.Printf("%s: Packument revision (_rev property) %s not in _changes feed. CouchDB says %s", event.Change.ID, p.Rev, event.Change.Changes[0].Rev)
			}
			// do something with the Packument here -- this example gets the latest
			// version (Latest() convenience utility and logs version manifest metadata)
			latest := p.Latest()
			log.Printf("%s: updated - latest version %s was published by npm user %s (%s)", event.Change.ID, latest.Version, latest.NpmUser.Name, latest.NpmUser.Email)
		})

	}
	// Wait for all the 10-second timers to finish before quitting completely
	log.Println("shutting down, waiting for pending workers...")
	wg.Wait()
}

```

On top of replication unreliability, while I've made a best-effort attempt to create a Packument unmarshaler, there really isn't much of a strict standard and they come in all shapes and sizes. Therefore, you may hit unmarshalling issues. In cases where you must not face unmarshalling errors, use the Fetch* utility functions, which return the response.Body for you to use as-is.