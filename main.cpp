#include <iostream>
#include <queue>
#include <set>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <fstream>
#include <list>
#include <sstream>
#include <unistd.h>
#include <chrono>
#include <random>
#include "json.hpp"
using json = nlohmann::json;

using namespace std;

/*
 * Atomic stuff for the orderQueue
 */
mutex ordersMutex, deliveryMutex, printMutex, foodWaitTimeMutex;;
condition_variable orderObtained, deliveryReady;

double totalFoodWaitTimeAcrossALlOrders_needsLockToAccess = 0;

class Order {
public:
    Order() : _id(""), _name(""), _prepTime(0) {}
    Order(string id, string name, int prepTime) : _id(id), _name(name), _prepTime(prepTime) {}
    Order(const Order &obj) : _id(obj._id), _name(obj._name), _prepTime(obj._prepTime) {}
    void operator=(const Order& obj) { _id = obj._id; _name = obj._name; _prepTime = obj._prepTime; };

    void show() {
        cout << "id=" << _id << ", name=" << _name << ", prepTime=" << _prepTime << endl;
    }
    string orderId() const { return _id; }
    unsigned prepTime() const { return _prepTime; }
private:
    string _id;
    string _name;
    unsigned _prepTime;
};

void addToTotalFoodWaitTime(double time);

enum orderTakerType {
    type_fifo,
    type_matched
};
static void atomicPrint(const string& toPrint)
{
    unique_lock<mutex> lock(printMutex);
    cout << toPrint;
}

double millisecondsElapsed(chrono::system_clock::time_point start)
{
    typedef std::chrono::high_resolution_clock clock;
    typedef std::chrono::duration<float, std::milli> duration;

    duration elapsed = chrono::system_clock::now() - start;
    return elapsed.count();
}
/*
 * Here is the shared order queue.
 * Orders show up in this queue.
 */
list<Order> orderPickupQueue_needsLockToAccess;
list<Order> fifoDeliveryQueue_needsLockToAccess;
unordered_map<string, chrono::time_point<chrono::system_clock>> orderReadyForDelivery;              // This map is used to tell the matched courier that "HIS" order is ready to be delivered.

/*
 * This function parses the given jsonFile and outputs the json objects a list of Orders.
 */
list<Order> parseJsonIntoOrderList(string filename) {
    list<Order> orderList;
    std::ifstream f(filename);
    json objects = json::parse(f);
    // using a list here to be able to remove elements from the front, thereby ensuring constant complexity whie doing so
    for (auto it = objects.begin(); it != objects.end(); ++it) {
        string id = "";
        string name = "";
        unsigned prepTime;
        for(auto iit : it->items()) {
            if (iit.key() == "id") {
                id = iit.value();
            } else if (iit.key() == "name") {
                name = iit.value();
            } else if (iit.key() == "prepTime") {
                prepTime = iit.value();
            }
        }
        orderList.push_back(Order(id, name, prepTime));
    }
    return orderList;
}

int intRand(const int & min, const int & max) {
    thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<int> distribution(min,max);
    return distribution(generator);
}

void courier(orderTakerType takerType, Order order)
{
    string courierName = (takerType == type_fifo) ? "CourierFIFO" : "CourierMatched";

    int arrivalTimeOfTheCourier = (intRand(0,1091) % 13) + 3;
    sleep(arrivalTimeOfTheCourier);
    vector<string> messages;

    stringstream ss;
    ss << courierName << " - arriving at time t=" << arrivalTimeOfTheCourier << " post dispatch" << endl;
    messages.push_back(ss.str());

    {
        unique_lock<mutex> orderDeliveryLock(deliveryMutex);
        if(takerType == type_matched) {
            // check if the order id is in the set
            while(orderReadyForDelivery.find(order.orderId()) == orderReadyForDelivery.end()) {
                deliveryReady.wait(orderDeliveryLock);
            }

            // here we know that the order is ready to be delivered.
            double foodWaitTime = millisecondsElapsed(orderReadyForDelivery[order.orderId()]);
            addToTotalFoodWaitTime(foodWaitTime);
            stringstream ss;
            ss << courierName << " - delivering order=" << order.orderId() << " foodWaitTime=" << foodWaitTime << endl;
            messages.push_back(ss.str());

            orderReadyForDelivery.erase(orderReadyForDelivery.find(order.orderId()));   // this order has been picked up and delivered.
        } else {
            assert(takerType == type_fifo);
            while(fifoDeliveryQueue_needsLockToAccess.empty()) {deliveryReady.wait(orderDeliveryLock);}

            /*
             * Order obtained. Processing
             */
            Order orderTODeliver = fifoDeliveryQueue_needsLockToAccess.front();
            fifoDeliveryQueue_needsLockToAccess.pop_front();
            stringstream ss;
            ss << courierName << " - delivering order=" << orderTODeliver.orderId() << endl;
            messages.push_back(ss.str());
        }
    }

    /*
     * Dump the messages for debugging
     */
    for (string s : messages) atomicPrint(s);
}

void addToTotalFoodWaitTime(double time)
{
    unique_lock<mutex> foodWaitTimeLock(foodWaitTimeMutex);
    totalFoodWaitTimeAcrossALlOrders_needsLockToAccess +=  time;
}

void orderTaker(orderTakerType takerType, unsigned totalAnticipatedOrders)
{
    string takerName = (takerType == type_fifo) ? "OrderTakerFIFO" : "OrderTakerMATCHED";
    stringstream ss;ss  << "Order taker " << takerName << endl;
    atomicPrint(ss.str());

    unsigned processedOrders = 0;
    vector<thread> courierThreads;
    while(true) {
        if(processedOrders == totalAnticipatedOrders) {
            stringstream ss;
            ss << "OrderTakerFIFO - Completed Tasks - ProcessedOrders=" << processedOrders << endl;
            atomicPrint(ss.str());
            break;
        }
        Order order;
        {
            /*
             * Scope for the mutex
             */
            unique_lock<mutex> orderPickupLock(ordersMutex);
            while(orderPickupQueue_needsLockToAccess.empty()) {
                orderObtained.wait(orderPickupLock);
            }

            /*
             * Order obtained. Processing
             */
            order = orderPickupQueue_needsLockToAccess.front();
            orderPickupQueue_needsLockToAccess.pop_front();
        }
        ++processedOrders;
        thread courierObject = thread(courier, takerType, order);
        courierThreads.push_back(move(courierObject));

        /*
         * Prep the order, if it is matched, signal the couriers that Someone's order is ready.
         */
        if(takerType == type_matched) {
            // throw this order into the set so that the courier will see if his order is ready
            // wake up all couriers
            unique_lock<mutex> orderDeliveryLock(deliveryMutex);
            orderReadyForDelivery[order.orderId()] = chrono::system_clock::now();   // order was ready at this time.
            deliveryReady.notify_all();
        } else {
            assert(takerType == type_fifo);
            unique_lock<mutex> orderDeliveryLock(deliveryMutex);
            fifoDeliveryQueue_needsLockToAccess.push_back(order);
            deliveryReady.notify_all();
        }
    }

    // wait for couriers to be done.
    for (auto& t : courierThreads) {
        t.join();
    }
}

/*
 * Function to get two delivery orders per second into the system.
 * Essentially this is the order delivery logic. This deposits the orders into the order queue
 */
void orderGetter(list<Order> orderList)
{
    {
        stringstream ss;ss << "Order getter" << endl;
        atomicPrint(ss.str());
    }
    while (!orderList.empty())
    {
        {
            unique_lock<mutex> lock(ordersMutex);
            unsigned times = 2;
            while(times) {
                if(orderList.empty()) break;
                orderPickupQueue_needsLockToAccess.push_back(orderList.front());
                orderList.pop_front();
                --times;
            }
            orderObtained.notify_all();
        }
        sleep(1);
    }
    {
        stringstream ss;ss << "OrderGetter - exiting" << endl;
        atomicPrint(ss.str());
    }
}

int main()
{
    /*
     * Simulation start, start it with the correct orders we are getting.
     */
    list<Order> orderList = parseJsonIntoOrderList("dispatch_orders.json");
    unsigned totalOrdersAnticipated = orderList.size();

    /*
     * This thread is responsible for getting orders
     */
    thread orderGetterThreadObject = thread(orderGetter, orderList); // okay to make a copy, because of async executions
    thread orderTakerThreadObject = thread(orderTaker, type_matched, totalOrdersAnticipated);

    orderGetterThreadObject.join();
    orderTakerThreadObject.join();

    /*
     * Stats
     */
    cout << "Total Number of orders processed      = " << totalOrdersAnticipated  << endl;
    cout << "Total food wait time across orders    = " << totalFoodWaitTimeAcrossALlOrders_needsLockToAccess / 1000 << "seconds" << endl;
    cout << "Total average wait time across orders = " << (totalFoodWaitTimeAcrossALlOrders_needsLockToAccess / totalOrdersAnticipated)<< "ms" << endl;

    return 0;
}
