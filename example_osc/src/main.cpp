#include "ofAppNoWindow.h"
#include "ofMain.h"

#include "ofApp.h"

int main(int argc, char * argv[]) {
    std::vector<std::string> args(argv + 1, argv + argc);

    // With no display (over ssh, or a Pi without a desktop) it runs headless:
    // GPIO and the console only, with keys typed on stdin.
    bool headless = std::find(args.begin(), args.end(), "--headless") != args.end() ||
                    (!getenv("DISPLAY") && !getenv("WAYLAND_DISPLAY"));

    // The window comes first: the app's members may use oF as they're built.
    std::shared_ptr<ofAppBaseWindow> window;
    if (headless) {
        ofInit();
        auto noWindow = std::make_shared<ofAppNoWindow>();
        ofGetMainLoop()->addWindow(noWindow);
        window = noWindow;
    } else {
        ofGLWindowSettings settings;
        settings.setSize(1024, 600);
        settings.windowMode = OF_WINDOW;
        window = ofCreateWindow(settings);
    }
    ofRunApp(window, std::make_shared<ofApp>(args, headless));
    return ofRunMainLoop();
}
