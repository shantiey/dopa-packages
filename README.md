data market operators
=============

Patterns of data set by datamarket:

for the Access API 
		    
    	  	   |<------- ds-part ------------------------->|
    		   |<----set-part----->|<------ set-part------>|
    		  set-id dim-id val-id |separate      separate |
    		    |     |      |     |dimensions    dim/value|
    	            |     |      |     |    |             |    |
  	url +   ?ds=17tm!kqc=t.3.12.s.j/12rb!e4s=1a.1y:e4t=2.3.5

Visualisation API:

                               set-id(datasets)  dim-id  val-id
                                     |            |      |
                                     |            |      |
https://datamarket.com/lod/datasets/17tm/view/ds-kqc-t.3.12.s.j
				                |          |
				              separate    separate
				              dimval-part  val-id 

                  
                  the response is a html page
