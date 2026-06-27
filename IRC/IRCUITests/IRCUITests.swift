import XCTest

final class IRCUITests: XCTestCase {

    var app: XCUIApplication!

    override func setUpWithError() throws {
        continueAfterFailure = false
        app = XCUIApplication()
        app.launch()
    }

    func testSidebarShowsAddServerButton() throws {
        // The "+" toolbar button should be visible
        let addButton = app.buttons["Add server"]
        // On iPad it's in the sidebar; on iPhone it's in the navigation bar
        XCTAssert(
            app.navigationBars.buttons.element(boundBy: 1).exists
            || addButton.exists
        )
    }

    func testWelcomeContentUnavailableViewShows() throws {
        XCTAssert(app.staticTexts["Welcome to IRC"].exists || app.staticTexts["Select a Server"].exists)
    }
}
