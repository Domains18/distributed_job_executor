#Distributed Job Execution Engine

- A zero dependency system that accepts jobs from an e-commerce platform and reliably executes millions of background tasks across a cluster of workers

- I built this project as a learning point for a recent interview I went through which I saw stars. So I will be documenting most of these folders but they are mostly my own personal notes; especially the storage part

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