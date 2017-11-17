
#include <iostream>
#include <string>

#include <boost/lexical_cast.hpp>

#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>
#include <mesos/type_utils.hpp>

#include <stout/check.hpp>
#include <stout/exit.hpp>
#include <stout/flags.hpp>
#include <stout/numify.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/stringify.hpp>
#include<queue>

#include "logging/flags.hpp"
#include "logging/logging.hpp"
#include<memory>

using namespace mesos;

using boost::lexical_cast;

using std::cerr;
using std::cout;
using std::endl;
using std::flush;
using std::string;
using std::vector;

using mesos::Resources;

const int32_t CPUS_PER_TASK = 1;
const int32_t MEM_PER_TASK = 128;

constexpr char EXECUTOR_BINARY[] = "my_executor";
constexpr char EXECUTOR_NAME[] = "My Executor (C++)";
constexpr char FRAMEWORK_NAME[] = "My  Framework (C++)";
constexpr char FRAMEWORK_PRINCIPAL[] = "my-framework-cpp";

class Job
{

public:
  enum Goal
  {
    CONTINUER,
    ONCE
  };

  enum State
  {
     INIT,
     RUNNING,
     FINISHED
  };

  Job(int _task_id, std::shared_ptr<TaskInfo> _my_mesos_task_info, Goal _my_goal,
  std::string& _uri,
  std::string _command, State _my_state): task_id(_task_id), my_mesos_task_info(_my_mesos_task_info),
  my_goal(_my_goal), uri(_uri), command(_command), my_state(_my_state)
  {}

  size_t gettaskid() const
  {
    return task_id;
  }

  const std::shared_ptr<TaskInfo>& gettaskiinfo() const
  {
    return my_mesos_task_info;
  }

  const Goal& getgoal() const
  {
    return my_goal;
  }

  const std::string& geturi() const
  {
    return uri;
  }

  const std::string& getcommand() const
  {
    return command;
  }

  const State& getstate() const
  {
    return my_state;
  }

  void setstate(const State& _state) 
  {
    my_state = _state;
  }

private:
  size_t task_id;
  std::shared_ptr<TaskInfo> my_mesos_task_info;
  Goal my_goal;
  std::string uri;
  std::string command;
  State my_state;
};

class MyCustomSchduler : public Scheduler
{
public:
  MyCustomSchduler(
      bool _implicitAcknowledgements,
      const ExecutorInfo& _executor,
      const string& _role)
    : implicitAcknowledgements(_implicitAcknowledgements),
      executor(_executor),
      role(_role),
      tasksLaunched(0),
      tasksFinished(0),
      totalTasks(2) 
      {

          my_job_list.resize(totalTasks);
          Job::Goal goal = Job::CONTINUER;
          std::string uri;
          std::string command;
          Job::State my_state=Job::State::INIT;
          for ( int i=0; i<totalTasks; ++i )
          {
              std::shared_ptr<TaskInfo> task(new TaskInfo());
              task->set_name("Task " + lexical_cast<string>(i+1));
              task->mutable_task_id()->set_value(lexical_cast<string>(i+1));

              if (i==1)
              {
                goal = Job::ONCE;
              }

              std::shared_ptr<Job> my_job( new Job (i,task, goal, uri, command, my_state));
              my_job_list[i]=my_job;
              //task.mutable_slave_id()->MergeFrom(offer.slave_id());
              //task.mutable_executor()->MergeFrom(executor);

          }
          cout<<"done"<<endl;
      }

  virtual ~MyCustomSchduler() {}

  virtual void registered(SchedulerDriver*,
                          const FrameworkID&,
                          const MasterInfo&)
  {
    cout << "Registered!" << endl;
  }

  virtual void reregistered(SchedulerDriver*, const MasterInfo& masterInfo) {}

  virtual void disconnected(SchedulerDriver* driver) {}

  virtual void resourceOffers(SchedulerDriver* driver,
                              const vector<Offer>& offers)
  {
    cout<<"resourceOffers"<<endl;
    foreach (const Offer& offer, offers) 
    {
      cout << "Received offer " << offer.id() << " with " << offer.resources()
           << endl;

      Resources taskResources = Resources::parse(
          "cpus:" + stringify(CPUS_PER_TASK) +
          ";mem:" + stringify(MEM_PER_TASK)).get();
      taskResources.allocate(role);

      Resources remaining = offer.resources();

      // Launch tasks.
      vector<TaskInfo> tasks_to_launch;
      

      while (tasksLaunched < totalTasks &&
             remaining.toUnreserved().contains(taskResources)) 
      {
        tasksLaunched++;

        for (int i=0; i< totalTasks; ++i)
        {


          std::shared_ptr<Job> job= my_job_list[i];

          if ( job->getstate()==Job::INIT )
          {
          //task.set_name("Task " + lexical_cast<string>());
          //task.mutable_task_id()->set_value(lexical_cast<string>(taskId));
            job->gettaskiinfo()->mutable_slave_id()->MergeFrom(offer.slave_id());
            job->gettaskiinfo()->mutable_executor()->MergeFrom(executor);

            cout << "Launching task " << job->gettaskiinfo()->task_id() << " using offer "
                 << offer.id() << endl;

            Option<Resources> resources = [&]() 
            {
              if (role == "*") {
                return remaining.find(taskResources);
              }

              Resource::ReservationInfo reservation;
              reservation.set_type(Resource::ReservationInfo::STATIC);
              reservation.set_role(role);

              return remaining.find(taskResources.pushReservation(reservation));
            }();

            CHECK_SOME(resources);
            job->gettaskiinfo()->mutable_resources()->MergeFrom(resources.get());
            remaining -= resources.get();
            job->setstate(Job::RUNNING);
            tasks_to_launch.push_back(*(job->gettaskiinfo().get()));
          }
        }
      }

      driver->launchTasks(offer.id(), tasks_to_launch);
    }
  }

  virtual void offerRescinded(SchedulerDriver* driver, const OfferID& offerId)
  {}

  virtual void statusUpdate(SchedulerDriver* driver, const TaskStatus& status)
  {
    int taskId = lexical_cast<int>(status.task_id().value());

    cout << "Task " << taskId << " is in state " << status.state() << endl;

    //if ( status.state()== )
    std::shared_ptr<Job> my_job = my_job_list[taskId-1];
    if (status.state() == TASK_FINISHED) {

      if (my_job->getgoal()==Job::CONTINUER)
      {
        tasksLaunched--;
        my_job->setstate(Job::INIT);
      }
      else
      {
        tasksFinished++;
        cout<< "Task Finished"<< tasksFinished <<endl;
        my_job->setstate(Job::FINISHED);
      }
    }

    if (status.state() == TASK_LOST ||
        status.state() == TASK_KILLED ||
        status.state() == TASK_FAILED) {
      cout << "Aborting because task " << taskId
           << " is in unexpected state " << status.state()
           << " with reason " << status.reason()
           << " from source " << status.source()
           << " with message '" << status.message() << "'" << endl;
      driver->abort();
    }

    if (!implicitAcknowledgements) {
      driver->acknowledgeStatusUpdate(status);
    }

    if (tasksFinished == totalTasks) {
      driver->stop();
    }
  }

  virtual void frameworkMessage(
      SchedulerDriver* driver,
      const ExecutorID& executorId,
      const SlaveID& slaveId,
      const string& data)
  {}

  virtual void slaveLost(SchedulerDriver* driver, const SlaveID& sid) {}

  virtual void executorLost(
      SchedulerDriver* driver,
      const ExecutorID& executorID,
      const SlaveID& slaveID,
      int status)
  {}

  virtual void error(SchedulerDriver* driver, const string& message)
  {
    cout << message << endl;
  }

private:
  const bool implicitAcknowledgements;
  const ExecutorInfo executor;
  string role;
  int tasksLaunched;
  int tasksFinished;
  int totalTasks;
  std::vector<std::shared_ptr<Job> > my_job_list;
};

void usage(const char* argv0, const flags::FlagsBase& flags)
{
  cerr << "Usage: " << Path(argv0).basename() << " [...]" << endl
       << endl
       << "Supported options:" << endl
       << flags.usage();
}

class Flags : public virtual mesos::internal::logging::Flags
{
public:
  Flags()
  {
    add(&Flags::role, "role", "Role to use when registering", "*");
    add(&Flags::master, "master", "ip:port of master to connect");
  }

  string role;
  Option<string> master;
};

int main(int argc, char** argv)
{
  // Find this executable's directory to locate executor.
  string uri;
  Option<string> value = os::getenv("MESOS_HELPER_DIR");
  if (value.isSome()) {
    uri = path::join(value.get(), EXECUTOR_BINARY);
  } else {
    uri =
      path::join(os::realpath(Path(argv[0]).dirname()).get(), EXECUTOR_BINARY);
  }

  cout<<"URI="<<uri<<endl;
  Flags flags;

  Try<flags::Warnings> load = flags.load(None(), argc, argv);
  if (load.isError()) {
    cerr << load.error() << endl;
    usage(argv[0], flags);
    exit(EXIT_FAILURE);
  } else if (flags.master.isNone()) {
    cerr << "Missing --master" << endl;
    usage(argv[0], flags);
    exit(EXIT_FAILURE);
  }

  internal::logging::initialize(argv[0], flags, true); // Catch signals.
  // Log any flag warnings (after logging is initialized).
  foreach (const flags::Warning& warning, load->warnings) {
    LOG(WARNING) << warning.message;
  }

  ExecutorInfo executor;
  executor.mutable_executor_id()->set_value("default");
  executor.mutable_command()->set_value(uri);
  executor.set_name(EXECUTOR_NAME);

  FrameworkInfo framework;
  framework.set_user(""); // Have Mesos fill in the current user.
  framework.set_name(FRAMEWORK_NAME);
  framework.set_role(flags.role);
  framework.add_capabilities()->set_type(
      FrameworkInfo::Capability::RESERVATION_REFINEMENT);

  value = os::getenv("MESOS_CHECKPOINT");
  if (value.isSome()) {
    framework.set_checkpoint(numify<bool>(value.get()).get());
  }

  bool implicitAcknowledgements = true;
  if (os::getenv("MESOS_EXPLICIT_ACKNOWLEDGEMENTS").isSome()) {
    cout << "Enabling explicit acknowledgements for status updates" << endl;

    implicitAcknowledgements = false;
  }

  MesosSchedulerDriver* driver;
  MyCustomSchduler scheduler(implicitAcknowledgements, executor, flags.role);

  if (os::getenv("MESOS_AUTHENTICATE_FRAMEWORKS").isSome()) {
    cout << "Enabling authentication for the framework" << endl;

    value = os::getenv("DEFAULT_PRINCIPAL");
    if (value.isNone()) {
      EXIT(EXIT_FAILURE)
        << "Expecting authentication principal in the environment";
    }

    Credential credential;
    credential.set_principal(value.get());

    framework.set_principal(value.get());

    value = os::getenv("DEFAULT_SECRET");
    if (value.isNone()) {
      EXIT(EXIT_FAILURE)
        << "Expecting authentication secret in the environment";
    }

    credential.set_secret(value.get());

    driver = new MesosSchedulerDriver(
        &scheduler,
        framework,
        flags.master.get(),
        implicitAcknowledgements,
        credential);
  } else {
    framework.set_principal(FRAMEWORK_PRINCIPAL);

    driver = new MesosSchedulerDriver(
        &scheduler,
        framework,
        flags.master.get(),
        implicitAcknowledgements);
  }

  int status = driver->run() == DRIVER_STOPPED ? 0 : 1;

  // Ensure that the driver process terminates.
  driver->stop();

  delete driver;
  return status;
}
