#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/workqueue.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME "container_monitor"
#define CHECK_INTERVAL_SEC 1

struct monitored_process {
    pid_t pid;
    char container_id[MONITOR_NAME_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int soft_warning_emitted;
    struct list_head list;
};

static LIST_HEAD(monitored_list);
static DEFINE_MUTEX(monitored_lock);

static struct workqueue_struct *monitor_wq;
static struct delayed_work monitor_work;
static dev_t dev_num;
static struct cdev c_dev;
static struct class *cl;

/* Return RSS memory in bytes for a PID, or -1 if the task is unavailable. */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

/* Emit a kernel warning when a process crosses its soft memory limit. */
static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* Send SIGKILL to a process and log that the hard memory limit was exceeded. */
static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* Check registered processes and enforce soft/hard memory thresholds.
 * Runs in process context (workqueue) so mutex_lock is safe here. */
static void monitor_work_fn(struct work_struct *work)
{
    struct monitored_process *entry, *tmp;
    long rss_bytes;

    (void)work;

    mutex_lock(&monitored_lock);

    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        rss_bytes = get_rss_bytes(entry->pid);
        if (rss_bytes < 0) {
            list_del(&entry->list);
            kfree(entry);
            continue;
        }
        if ((unsigned long)rss_bytes > entry->hard_limit_bytes) {
            kill_process(entry->container_id, entry->pid,
                        entry->hard_limit_bytes, rss_bytes);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }
        if ((unsigned long)rss_bytes > entry->soft_limit_bytes &&
            !entry->soft_warning_emitted) {
            log_soft_limit_event(entry->container_id, entry->pid,
                                entry->soft_limit_bytes, rss_bytes);
            entry->soft_warning_emitted = 1;
        }
    }

    mutex_unlock(&monitored_lock);

    queue_delayed_work(monitor_wq, &monitor_work, CHECK_INTERVAL_SEC * HZ);
}

/* Handle monitor register/unregister ioctl requests from user space. */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {
        struct monitored_process *entry;

        printk(KERN_INFO
               "[container_monitor] Registering container=%s pid=%d soft=%lu hard=%lu\n",
               req.container_id, req.pid, req.soft_limit_bytes, req.hard_limit_bytes);

        if (req.soft_limit_bytes > req.hard_limit_bytes)
            return -EINVAL;
        entry = kmalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry)
            return -ENOMEM;
        entry->pid = req.pid;
        strncpy(entry->container_id, req.container_id, MONITOR_NAME_LEN - 1);
        entry->container_id[MONITOR_NAME_LEN - 1] = '\0';
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        entry->soft_warning_emitted = 0;
        INIT_LIST_HEAD(&entry->list);
        mutex_lock(&monitored_lock);
        list_add_tail(&entry->list, &monitored_list);
        mutex_unlock(&monitored_lock);

        return 0;
    }

    /* MONITOR_UNREGISTER */
    {
        struct monitored_process *entry, *tmp;
        int found = 0;

        printk(KERN_INFO
               "[container_monitor] Unregister request container=%s pid=%d\n",
               req.container_id, req.pid);

        mutex_lock(&monitored_lock);

        list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
            if (entry->pid == req.pid) {
                list_del(&entry->list);
                kfree(entry);
                found = 1;
                break;
            }
        }

        mutex_unlock(&monitored_lock);

        return found ? 0 : -ENOENT;
    }
}

static const struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* Initialize the monitor device, workqueue, and periodic scan task. */
static int __init monitor_init(void)
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    monitor_wq = create_singlethread_workqueue("container_monitor");
    if (!monitor_wq) {
        cdev_del(&c_dev);
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -ENOMEM;
    }
    INIT_DELAYED_WORK(&monitor_work, monitor_work_fn);
    queue_delayed_work(monitor_wq, &monitor_work, CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "[container_monitor] Module loaded. Device: /dev/%s\n", DEVICE_NAME);
    return 0;
}

/* Tear down monitor resources and unregister all tracked processes. */
static void __exit monitor_exit(void)
{
    struct monitored_process *entry, *tmp;

    cancel_delayed_work_sync(&monitor_work);
    destroy_workqueue(monitor_wq);

    mutex_lock(&monitored_lock);

    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }

    mutex_unlock(&monitored_lock);

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Guru R Bharadwaj, Harsh Pandya");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");
