/*
 * Copyright (c) 2019 Razeware LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * Notwithstanding the foregoing, you may not use, copy, modify, merge, publish,
 * distribute, sublicense, create a derivative work, and/or sell copies of the
 * Software in any work that is designed, intended, or marketed for pedagogical or
 * instructional purposes related to programming, coding, application development,
 * or information technology.  Permission for such use, copying, modification,
 * merger, publication, distribution, sublicensing, creation of derivative works,
 * or sale is expressly withheld.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.raywenderlich.android.cheesefinder

import android.text.Editable
import android.text.TextWatcher
import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import io.reactivex.Observable
import io.reactivex.android.schedulers.AndroidSchedulers
import io.reactivex.disposables.Disposable
import io.reactivex.schedulers.Schedulers
import kotlinx.android.synthetic.main.activity_cheeses.*
import java.util.concurrent.TimeUnit

class CheeseActivity : BaseSearchActivity() {

    private lateinit var disposable: Disposable


    // 1
    private fun createTextChangeObservable(): Observable<String> {
        // 2
        val textChangeObservable = Observable.create<String> { emitter ->
            // 3
            val textWatcher = object : TextWatcher {

                override fun afterTextChanged(s: Editable?) = Unit

                override fun beforeTextChanged(s: CharSequence?, start: Int, count: Int, after: Int) = Unit

                // 4
                override fun onTextChanged(s: CharSequence?, start: Int, count: Int, after: Int) {
                    s?.toString()?.let { emitter.onNext(it) }
                }
            }

            // 5
            queryEditText.addTextChangedListener(textWatcher)

            // 6
            emitter.setCancellable {
                queryEditText.removeTextChangedListener(textWatcher)
            }
        }

        // 7
        //return textChangeObservable
        //return textChangeObservable.filter { it.length >= 2 }
        return textChangeObservable
                .filter { it.length >= 2 }
                .debounce(1000, TimeUnit.MILLISECONDS) // add this line

    }


    override fun onStart() {
        super.onStart()

        val buttonClickStream = createButtonClickObservable().toFlowable(BackpressureStrategy.LATEST)
        val textChangeStream = createTextChangeObservable().toFlowable(BackpressureStrategy.BUFFER)

        //val searchTextObservable = Observable.merge<String>(buttonClickStream, textChangeStream)
        val searchTextFlowable = Flowable.merge<String>(buttonClickStream, textChangeStream)

        //val searchTextObservable = createTextChangeObservable()

        //1
        //val searchTextObservable = createButtonClickObservable()


        disposable = searchTextFlowable
        //searchTextObservable
                .subscribeOn(AndroidSchedulers.mainThread())
                .observeOn(AndroidSchedulers.mainThread())
                .doOnNext { showProgress() }
                .observeOn(Schedulers.io())
                .map {
                    val result = cheeseSearchEngine.search(it)
                    return@map if (result!=null) result else emptyList()
                }
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe {
                    hideProgress()
                    showResult(it)
                }


    }

    @Override
    override fun onStop() {
        super.onStop()
        if (!disposable.isDisposed) {
            disposable.dispose()
        }
    }

    //1
    private fun createButtonClickObservable():Observable<String> {
        //2
        return Observable.create { emitter ->
            //3
            searchButton.setOnClickListener {
                //4

                emitter.onNext(queryEditText.text.toString())
            }

            //5
            emitter.setCancellable {
                //6
                searchButton.setOnClickListener (null)
            }

        }
    }


}