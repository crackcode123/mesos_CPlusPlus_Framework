
#include <iostream>
//#include <boost/regex.hpp>

#include <stout/lambda.hpp>
#include <mesos/executor.hpp>

#include <stout/duration.hpp>
#include <stout/os.hpp>
#include <unistd.h>

using namespace mesos;

using std::cout;
using std::endl;
using std::string;

static void runTask(ExecutorDriver* driver, const TaskInfo& task)
{
    cout << "running hello and sleep" <<endl;
    sleep(60);  
    cout << "Finishing task " << task.task_id().value() << endl;

    TaskStatus status;
    status.mutable_task_id()->MergeFrom(task.task_id());
    status.set_state(TASK_FINISHED);

    driver->sendStatusUpdate(status);
}

void* start(void* arg)
{
  lambda::function<void(void)>* thunk = (lambda::function<void(void)>*) arg;
  (*thunk)();
  delete thunk;
  return NULL;
}


class MyCustomExecutor : public Executor
{
public:
  virtual ~MyCustomExecutor() {}

  virtual void registered(ExecutorDriver* driver,
                          const ExecutorInfo& executorInfo,
                          const FrameworkInfo& frameworkInfo,
                          const SlaveInfo& slaveInfo)
  {
    cout << "Registered executor on " << slaveInfo.hostname() << endl;
  }

  virtual void reregistered(ExecutorDriver* driver,
                            const SlaveInfo& slaveInfo)
  {
    cout << "Re-registered executor on " << slaveInfo.hostname() << endl;
  }

  virtual void disconnected(ExecutorDriver* driver) {}

  virtual void launchTask(ExecutorDriver* driver, const TaskInfo& task)
  {
    cout << "Starting task " << task.task_id().value() << endl;

      lambda::function<void(void)>* thunk =
        new lambda::function<void(void)>(lambda::bind(&runTask, driver, task));

      pthread_t pthread;
      if (pthread_create(&pthread, NULL, &start, thunk) != 0) {
        TaskStatus status;
        status.mutable_task_id()->MergeFrom(task.task_id());
        status.set_state(TASK_FAILED);
        driver->sendStatusUpdate(status);
      } else {
        pthread_detach(pthread);
        TaskStatus status;
        status.mutable_task_id()->MergeFrom(task.task_id());
        status.set_state(TASK_RUNNING);
        driver->sendStatusUpdate(status);
      }
#if 0
    TaskStatus status;
    status.mutable_task_id()->MergeFrom(task.task_id());
    status.set_state(TASK_RUNNING);

    driver->sendStatusUpdate(status);
    //status.mutable_task_id()->MergeFrom(task.task_id());

    // This is where one would perform the requested task.

    cout << "running hello";
    sleep(100);
    cout << "Finishing task " << task.task_id().value() << endl;

    TaskStatus status;
    status.mutable_task_id()->MergeFrom(task.task_id());
    status.set_state(TASK_FINISHED);

    driver->sendStatusUpdate(status);
#endif
  }

  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId) {}
  virtual void frameworkMessage(ExecutorDriver* driver, const string& data) {}
  virtual void shutdown(ExecutorDriver* driver) {}
  virtual void error(ExecutorDriver* driver, const string& message) {}
};


int main(int argc, char** argv)
{
  MyCustomExecutor executor;
  MesosExecutorDriver driver(&executor);
  return driver.run() == DRIVER_STOPPED ? 0 : 1;
}
