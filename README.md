# ZIO challenge

In this drill i've implemtented Sliding Window Aggregations (SWAG)
inside i use SWAG so know it refers to it.


* the current dictionary is: event_types = ["baz", "dolor", "bar"], event_data = ["ipum", "amet", "shuki"]

* when running, to get the frequency of word event by type in the recent window issue a GET req to localhost:8080/type/[type]<br>
     > localhost:8080/type/bar

* inside you can define both the window size and the slide - both in ZIO.Duration

* the blackbox data generator is implemented inside with a ZStream.repeat

* this drill was implemented using ZIO 1.0.15 (+zio-json, http4s) to be able to first expirience how ZIO was before all improvment of ZIO 2.0

* The Sate Service implemtend inside is not yet used so please ignore it 

* i've implmented a simple SWAG algorithm using partial aggregations called slices. 
  at the begining we first get the GCP of the window and slide sizes to understand how many slices we will have both in a slide and in a 
window

* **for example:**<br>
     > window of 18.seconds, slide 3.seconds the slice size will be 3.second<br>
     > window of 18.seconds, slide 4.seconds the slice size will be 2.second<br>
     > window of 20.seconds, slide 3.seconds the slice size will be 1.second<br>

* the hight the GCD(window,slice) the least number of slices in a slide/window thus the faster a window will be aggregated.
* so for example for large windows e.g. hours windows can be computed very fast if we have few slices in a slide.
* when Window % Slide = 0 => Slide is a single slice
* slices and window are printed to stdout

* the algorithm always compute slices first. it takes messages from blackbox stream according to event time extracted until it feels a slice

* it then pushes the slice to a state which is a Queue of Slices
* Slice = Map[(String,String),Int] e.g. (bar,ipum) -> 4 meaning in this slice there were 4 messages with event_type: bar and data: ipum

