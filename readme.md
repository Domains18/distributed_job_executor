#Distributed Job Execution Engine

- A zero dependency system that accepts jobs from an e-commerce platform and reliably executes millions of background tasks across a cluster of workers



## architecture
                    Client
                      │
                      ▼
                ┌───────────┐
                │ Job API   │
                └─────┬─────┘
                      │
                      ▼
              ┌──────────────┐
              │   Scheduler  │
              └──────┬───────┘
                     │
              ┌──────▼───────┐
              │ Queue Broker │
              └──────┬───────┘
                     │
       ┌─────────────┼─────────────┐
       ▼             ▼             ▼
   Worker A      Worker B      Worker C
       │             │             │
       └─────────────┼─────────────┘
                     ▼
              ┌──────────────┐
              │ PostgreSQL   │
              └──────────────┘