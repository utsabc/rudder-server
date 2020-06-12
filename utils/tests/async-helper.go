package testutils

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/onsi/ginkgo"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// AsyncTestHelper provides synchronization methods to test goroutines.
// Example:
//		var (
// 			asyncHelper testutils.AsyncTestHelper
// 		)
//
//		BeforeEach(func() {
//			mockMyInterface.EXPECT().MyMethodInGoroutine().Do(asyncHelper.ExpectAndNotifyCallback())
//		})
//
// 		AfterEach(func() {
//			asyncHelper.WaitWithTimeout(time.Second)
//		})
type AsyncTestHelper struct {
	wg sync.WaitGroup
}

// ExpectAndNotifyCallback Adds one to this helper's WaitGroup, and provides a callback that calls Done on it.
// Should be used for gomock Do calls that trigger via mocked functions executed in a goroutine.
func (helper *AsyncTestHelper) ExpectAndNotifyCallback() func(...interface{}) {
	return helper.ExpectAndNotifyCallbackTimes(1)
}

// TODO: Update the comment
// ExpectAndNotifyCallbackTimes Adds one to this helper's WaitGroup, and provides a callback that calls Done on it.
// Should be used for gomock Do calls that trigger via mocked functions executed in a goroutine.
func (helper *AsyncTestHelper) ExpectAndNotifyCallbackTimes(times int) func(...interface{}) {
	helper.wg.Add(times)
	return func(...interface{}) {
		helper.wg.Done()
	}
}

// WaitWithTimeout waits for this helper's WaitGroup until provided timeout.
// Should wait for all ExpectAndNotifyCallback callbacks, registered in asynchronous mocks calls
func (helper *AsyncTestHelper) WaitWithTimeout(d time.Duration) {
	helper.WaitWithTimeoutName("", d)
}

// WaitWithTimeoutName waits for this helper's WaitGroup until provided timeout.
// Should wait for all ExpectAndNotifyCallback callbacks, registered in asynchronous mocks calls
func (helper *AsyncTestHelper) WaitWithTimeoutName(name string, d time.Duration) {
	RunTestWithTimeoutName(func() {
		helper.wg.Wait()
	}, name, d)
}

// RegisterCalls registers a number of calls to this async helper, so that they are waited for.
func (helper *AsyncTestHelper) RegisterCalls(calls ...*gomock.Call) {
	for _, call := range calls {
		call.Do(helper.ExpectAndNotifyCallback())
	}
}

// RunTestWithTimeoutName runs function f until provided timeout.
// If the function times out, it will cause the ginkgo test to fail.
func RunTestWithTimeoutName(f func(), name string, d time.Duration) {
	misc.RunWithTimeout(func() {
		defer ginkgo.GinkgoRecover()
		f()
	}, func() {
		ginkgo.Fail(fmt.Sprintf("Async helper's wait group timed out: %s", name))
	}, d)
}

// RunTestWithTimeout runs function f until provided timeout.
// If the function times out, it will cause the ginkgo test to fail.
func RunTestWithTimeout(f func(), d time.Duration) {
	RunTestWithTimeoutName(f, "", d)
}
