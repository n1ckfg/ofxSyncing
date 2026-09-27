#pragma once

// One GPIO output, driven through the Linux GPIO character device (the v2
// uAPI), so no library is needed. On a Pi 4 the header pins are lines of
// /dev/gpiochip0, numbered as BCM GPIOs; on a Pi 5 they're on the chip
// labelled pinctrl-rp1 (gpioinfo lists them). The user needs to be in the
// gpio group, as pi is by default.

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>

#ifdef __linux__
#include <fcntl.h>
#include <linux/gpio.h>
#include <sys/ioctl.h>
#include <unistd.h>
#endif

class GpioPin {
public:
    ~GpioPin() {
        close();
    }

    bool open(const std::string & chip, int line) {
        close();
#ifdef __linux__
        int chipFd = ::open(chip.c_str(), O_RDWR | O_CLOEXEC);
        if (chipFd < 0) {
            error = chip + ": " + strerror(errno);
            return false;
        }

        gpio_v2_line_request request;
        memset(&request, 0, sizeof(request));
        request.offsets[0] = (uint32_t)line;
        request.num_lines = 1;
        request.config.flags = GPIO_V2_LINE_FLAG_OUTPUT;
        strncpy(request.consumer, "ofxSyncing", sizeof(request.consumer) - 1);

        int result = ioctl(chipFd, GPIO_V2_GET_LINE_IOCTL, &request);
        int err = errno;
        ::close(chipFd);
        if (result < 0) {
            error = chip + " line " + std::to_string(line) + ": " + strerror(err);
            return false;
        }
        fd = request.fd;
        set(false);
        return true;
#else
        error = "GPIO needs Linux";
        return false;
#endif
    }

    void set(bool high) {
#ifdef __linux__
        if (fd < 0) return;
        gpio_v2_line_values values;
        memset(&values, 0, sizeof(values));
        values.mask = 1;
        values.bits = high ? 1 : 0;
        ioctl(fd, GPIO_V2_LINE_SET_VALUES_IOCTL, &values);
#endif
    }

    void close() {
#ifdef __linux__
        if (fd >= 0) {
            set(false);
            ::close(fd);
            fd = -1;
        }
#endif
    }

    bool isOpen() const { return fd >= 0; }
    const std::string & getError() const { return error; }

private:
    int fd = -1;
    std::string error;
};
