
Flux.just(1)
    .map(v -> v+1)
    .subscribe();

Three phases:
- Assembly phase
  - Each step is built with a reference to the previous step (i.e. the source)

- Subscription phase
  - Each operator passes the subscribe() call to the "source", a Subscription object
  which carries the downstream subscriber (passed from the call .subscribe()).

  The very last subscription, done by subscribe() operator, subscribes using a LambdaSubscriber.

  When the source (e.g. Flux.Just) receives the subscribe() call, it internally calls onSubscribe and
  passes a Subscription object with subscriber callback and the values of the source.

  Eventually, the last subscription, receives onSubscribe(Subscription s) callback. This may call the subscriptionConsumer (if any), and more importantly the subcription.request(<???>).

  However, the object subscription was initially passed by the previous operator

- Delivery/Request phase
