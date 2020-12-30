/*
 * Copyright (c) 2017-2019 The Typelevel Cats-effect Project Developers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cats
package effect
package concurrent

import cats.data.State
import cats.effect.concurrent.RefError.TransformedRefError

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import cats.syntax.functor._
import cats.syntax.bifunctor._
import cats.syntax.flatMap._
import cats.syntax.either._

import scala.annotation.tailrec

/**
 * An asynchronous, concurrent mutable reference.
 *
 * Provides safe concurrent access and modification of its content, but no
 * functionality for synchronisation, which is instead handled by [[Deferred]].
 * For this reason, a `Ref` is always initialised to a value.
 *
 * The default implementation is nonblocking and lightweight, consisting essentially
 * of a purely functional wrapper over an `AtomicReference`.
 */
abstract class RefError[F[_], E, A] {

  /**
   * Obtains the current value.
   *
   * Since `Ref` is always guaranteed to have a value, the returned action
   * completes immediately after being bound.
   */
  def get: F[A]

  /**
   * Sets the current value to `a`.
   *
   * The returned action completes after the reference has been successfully set.
   *
   * Satisfies:
   *   `r.set(fa) *> r.get == fa`
   */
  def set(a: A): F[Unit]

  /**
   * Updates the current value using `f` and returns the previous value.
   *
   * In case of retries caused by concurrent modifications,
   * the returned value will be the last one before a successful update.
   */
  def getAndUpdate(f: A => A): F[A] = modify { a =>
    (f(a), a)
  }

  /**
   * Replaces the current value with `a`, returning the previous value.
   */
  def getAndSet(a: A): F[A] = getAndUpdate(_ => a)

  /**
   * Updates the current value using `f`, and returns the updated value.
   */
  def updateAndGet(f: A => A): F[A] = modify { a =>
    val newA = f(a)
    (newA, newA)
  }

  /**
   * Obtains a snapshot of the current value, and a setter for updating it.
   * The setter may noop (in which case `false` is returned) if another concurrent
   * call to `access` uses its setter first.
   *
   * Once it has noop'd or been used once, a setter never succeeds again.
   *
   * Satisfies:
   *   `r.access.map(_._1) == r.get`
   *   `r.access.flatMap { case (v, setter) => setter(f(v)) } == r.tryUpdate(f).map(_.isDefined)`
   */
  def access: F[(A, A => F[Boolean])]

  /**
   * Attempts to modify the current value once, returning `false` if another
   * concurrent modification completes between the time the variable is
   * read and the time it is set.
   */
  def tryUpdate(f: A => A): F[Boolean]

  /**
   * Like `tryUpdate` but allows the update function to return an output value of
   * type `B`. The returned action completes with `None` if the value is not updated
   * successfully and `Some(b)` otherwise.
   */
  def tryModify[B](f: A => (A, B)): F[Option[B]]

  /**
   * Modifies the current value using the supplied update function. If another modification
   * occurs between the time the current value is read and subsequently updated, the modification
   * is retried using the new value. Hence, `f` may be invoked multiple times.
   *
   * Satisfies:
   *   `r.update(_ => a) == r.set(a)`
   */
  def update(f: A => A): F[Unit]

  /**
   * Like `tryModify` but does not complete until the update has been successfully made.
   */
  def modify[B](f: A => (A, B)): F[B]

  /**
   * Update the value of this ref with a state computation.
   *
   * The current value of this ref is used as the initial state and the computed output state
   * is stored in this ref after computation completes. If a concurrent modification occurs,
   * `None` is returned.
   */
  def tryModifyState[B](state: State[A, B]): F[Option[B]]

  /**
   * Like [[tryModifyState]] but retries the modification until successful.
   */
  def modifyState[B](state: State[A, B]): F[B]

  /**
   * Modify the context `F` using transformation `f`.
   */
  def mapK[G[_]](f: F ~> G)(implicit F: Functor[F]): RefError[G, E, A] =
    new TransformedRefError(this, f)
}

object RefError {

  /**
   * Builds a `Ref` value for data types that are [[SyncError]]
   *
   * This builder uses the
   * [[https://typelevel.org/cats/guidelines.html#partially-applied-type-params Partially-Applied Type]]
   * technique.
   *
   * {{{
   *   RefError[IO].of(10) <-> Ref.of[IO, Int](10)
   * }}}
   *
   * @see [[of]]
   */
  def apply[F[_], E](implicit F: SyncError[F, E]): ApplyBuilders[F, E] = new ApplyBuilders(F)

  /**
   * Creates an asynchronous, concurrent mutable reference initialized to the supplied value.
   *
   * {{{
   *   import cats.effect.IO
   *   import cats.effect.concurrent.Ref
   *
   *   for {
   *     intRef <- Ref.of[IO, Int](10)
   *     ten <- intRef.get
   *   } yield ten
   * }}}
   *
   */
  def of[F[_], E, A](a: A)(implicit F: SyncError[F, E]): F[RefError[F, E, A]] = F.delay(unsafe(a))

  /**
   *  Builds a `Ref` value for data types that are [[SyncError]]
   *  Like [[of]] but initializes state using another effect constructor
   */
  def in[F[_], G[_], E, A](a: A)(implicit F: SyncError[F, E], G: SyncError[G, E]): F[RefError[G, E, A]] =
    F.delay(unsafe(a))

  /**
   * Like `apply` but returns the newly allocated ref directly instead of wrapping it in `F.delay`.
   * This method is considered unsafe because it is not referentially transparent -- it allocates
   * mutable state.
   *
   * This method uses the [[http://typelevel.org/cats/guidelines.html#partially-applied-type-params Partially Applied Type Params technique]],
   * so only effect type needs to be specified explicitly.
   *
   * Some care must be taken to preserve referential transparency:
   *
   * {{{
   *   import cats.effect.IO
   *   import cats.effect.concurrent.Ref
   *
   *   class Counter private () {
   *     private val count = Ref.unsafe[IO, Int](0)
   *
   *     def increment: IO[Unit] = count.update(_ + 1)
   *     def total: IO[Int] = count.get
   *   }
   *
   *   object Counter {
   *     def apply(): IO[Counter] = IO(new Counter)
   *   }
   * }}}
   *
   * Such usage is safe, as long as the class constructor is not accessible and the public one suspends creation in IO
   *
   * The recommended alternative is accepting a `RefError[F, E, A]` as a parameter:
   *
   * {{{
   *   class Counter (count: RefError[IO, Int]) {
   *     // same body
   *   }
   *
   *   object Counter {
   *     def apply(): IO[Counter] = RefError[IO](0).map(new Counter(_))
   *   }
   * }}}
   */
  def unsafe[F[_], E, A](a: A)(implicit F: SyncError[F, E]): RefError[F, E, A] =
    new SyncRefError[F, E, A](new AtomicReference[A](a))

  /**
   * Creates an instance focused on a component of another Ref's value.
   * Delegates every get and modification to underlying Ref, so both instances are always in sync.
   *
   * Example:
   *
   * {{{
   *   case class Foo(bar: String, baz: Int)
   *
   *   val refA: RefError[IO, Foo] = ???
   *   val refB: RefError[IO, String] =
   *     Ref.lens[IO, Foo, String](refA)(_.bar, (foo: Foo) => (bar: String) => foo.copy(bar = bar))
   * }}}
   * */
  def lens[F[_], E, A, B <: AnyRef](
    ref: RefError[F, E, A]
  )(get: A => B, set: A => B => A)(implicit F: SyncError[F, E]): RefError[F, E, B] =
    new LensRefError[F, E, A, B](ref)(get, set)

  final class ApplyBuilders[F[_], E](val F: SyncError[F, E]) extends AnyVal {

    /**
     * Creates an asynchronous, concurrent mutable reference initialized to the supplied value.
     *
     * @see [[Ref.of]]
     */
    def of[A](a: A): F[RefError[F, E, A]] = RefError.of(a)(F)
  }

  final private class SyncRefError[F[_], E, A](ar: AtomicReference[A])(implicit F: SyncError[F, E])
      extends RefError[F, E, A] {
    def get: F[A] = F.delay(ar.get)

    def set(a: A): F[Unit] = F.delay(ar.set(a))

    override def getAndSet(a: A): F[A] = F.delay(ar.getAndSet(a))

    def access: F[(A, A => F[Boolean])] = F.delay {
      val snapshot = ar.get
      val hasBeenCalled = new AtomicBoolean(false)
      def setter = (a: A) => F.delay(hasBeenCalled.compareAndSet(false, true) && ar.compareAndSet(snapshot, a))
      (snapshot, setter)
    }

    override def getAndUpdate(f: A => A): F[A] = {
      @tailrec
      def spin: A = {
        val a = ar.get
        val u = f(a)
        if (!ar.compareAndSet(a, u)) spin
        else a
      }
      F.delay(spin)
    }

    def tryUpdate(f: A => A): F[Boolean] =
      F.map(tryModify(a => (f(a), ())))(_.isDefined)

    def tryModify[B](f: A => (A, B)): F[Option[B]] = F.delay {
      val c = ar.get
      val (u, b) = f(c)
      if (ar.compareAndSet(c, u)) Some(b)
      else None
    }

    def update(f: A => A): F[Unit] = {
      @tailrec
      def spin(): Unit = {
        val a = ar.get
        val u = f(a)
        if (!ar.compareAndSet(a, u)) spin()
      }
      F.delay(spin())
    }

    override def updateAndGet(f: A => A): F[A] = {
      @tailrec
      def spin: A = {
        val a = ar.get
        val u = f(a)
        if (!ar.compareAndSet(a, u)) spin
        else u
      }
      F.delay(spin)
    }

    def modify[B](f: A => (A, B)): F[B] = {
      @tailrec
      def spin: B = {
        val c = ar.get
        val (u, b) = f(c)
        if (!ar.compareAndSet(c, u)) spin
        else b
      }
      F.delay(spin)
    }

    def tryModifyState[B](state: State[A, B]): F[Option[B]] = {
      val f = state.runF.value
      tryModify(a => f(a).value)
    }

    def modifyState[B](state: State[A, B]): F[B] = {
      val f = state.runF.value
      modify(a => f(a).value)
    }
  }

  implicit final class RefOps[F[_], E, A](private val ref: RefError[F, E, A]) extends AnyVal {

    /**
     * Like `update`, but can terminate early without retrying.
     * Returns whether or not the update short-circuited.
     */
    def updateMaybe(f: A => Option[A])(implicit F: Monad[F]): F[Boolean] =
      updateOr(a => f(a).toRight(())).map(_.isEmpty)

    /**
     * Like `modify`, but can terminate early without retrying.
     * Returns an optional value if the update completed.
     */
    def modifyMaybe[B](f: A => Option[(A, B)])(implicit F: Monad[F]): F[Option[B]] =
      modifyOr(a => f(a).toRight(())).map(_.toOption)

    /**
     * Like `update`, but can terminate early with an error value without retrying.
     * Returns an optional error if the update short-circuited.
     */
    def updateOr[L](f: A => Either[L, A])(implicit F: Monad[F]): F[Option[L]] =
      modifyOr(a => f(a).tupleRight(())).map(_.swap.toOption)

    /**
     * Like `modify`, but can terminate early with an error value without retrying.
     * Returns a value or error based on whether the update short-circuited.
     */
    def modifyOr[L, B](f: A => Either[L, (A, B)])(implicit F: Monad[F]): F[Either[L, B]] =
      ref.access.flatMap {
        case (a, set) =>
          f(a) match {
            case Right((a, b)) => set(a).ifM(ifTrue = F.pure(Right(b)), ifFalse = modifyOr(f))
            case l @ Left(_)   => F.pure(l.rightCast[B])
          }
      }

  }

  final private[concurrent] class TransformedRefError[F[_], G[_], E, A](underlying: RefError[F, E, A], trans: F ~> G)(
    implicit F: Functor[F]
  ) extends RefError[G, E, A] {
    override def get: G[A] = trans(underlying.get)
    override def set(a: A): G[Unit] = trans(underlying.set(a))
    override def getAndSet(a: A): G[A] = trans(underlying.getAndSet(a))
    override def tryUpdate(f: A => A): G[Boolean] = trans(underlying.tryUpdate(f))
    override def tryModify[B](f: A => (A, B)): G[Option[B]] = trans(underlying.tryModify(f))
    override def update(f: A => A): G[Unit] = trans(underlying.update(f))
    override def modify[B](f: A => (A, B)): G[B] = trans(underlying.modify(f))
    override def tryModifyState[B](state: State[A, B]): G[Option[B]] = trans(underlying.tryModifyState(state))
    override def modifyState[B](state: State[A, B]): G[B] = trans(underlying.modifyState(state))

    override def access: G[(A, A => G[Boolean])] =
      trans(F.compose[(A, *)].compose[A => *].map(underlying.access)(trans(_)))
  }

  final private[concurrent] class LensRefError[F[_], E, A, B <: AnyRef](underlying: RefError[F, E, A])(
    lensGet: A => B,
    lensSet: A => B => A
  )(implicit F: SyncError[F, E])
      extends RefError[F, E, B] {
    override def get: F[B] = F.map(underlying.get)(a => lensGet(a))

    override def set(b: B): F[Unit] = underlying.update(a => lensModify(a)(_ => b))

    override def getAndSet(b: B): F[B] = underlying.modify { a =>
      (lensModify(a)(_ => b), lensGet(a))
    }

    override def update(f: B => B): F[Unit] =
      underlying.update(a => lensModify(a)(f))

    override def modify[C](f: B => (B, C)): F[C] =
      underlying.modify { a =>
        val oldB = lensGet(a)
        val (b, c) = f(oldB)
        (lensSet(a)(b), c)
      }

    override def tryUpdate(f: B => B): F[Boolean] =
      F.map(tryModify(a => (f(a), ())))(_.isDefined)

    override def tryModify[C](f: B => (B, C)): F[Option[C]] =
      underlying.tryModify { a =>
        val oldB = lensGet(a)
        val (b, result) = f(oldB)
        (lensSet(a)(b), result)
      }

    override def tryModifyState[C](state: State[B, C]): F[Option[C]] = {
      val f = state.runF.value
      tryModify(a => f(a).value)
    }

    override def modifyState[C](state: State[B, C]): F[C] = {
      val f = state.runF.value
      modify(a => f(a).value)
    }

    override val access: F[(B, B => F[Boolean])] =
      F.flatMap(underlying.get) { snapshotA =>
        val snapshotB = lensGet(snapshotA)
        val setter = F.delay {
          val hasBeenCalled = new AtomicBoolean(false)

          (b: B) => {
            F.flatMap(F.delay(hasBeenCalled.compareAndSet(false, true))) { hasBeenCalled =>
              F.map(underlying.tryModify { a =>
                if (hasBeenCalled && (lensGet(a) eq snapshotB))
                  (lensSet(a)(b), true)
                else
                  (a, false)
              })(_.getOrElse(false))
            }
          }
        }
        setter.tupleLeft(snapshotB)
      }

    private def lensModify(s: A)(f: B => B): A = lensSet(s)(f(lensGet(s)))
  }

  implicit def catsInvariantForRef[F[_]: Functor, E]: Invariant[RefError[F, E, *]] =
    new Invariant[RefError[F, E, *]] {
      override def imap[A, B](fa: RefError[F, E, A])(f: A => B)(g: B => A): RefError[F, E, B] =
        new RefError[F, E, B] {
          override val get: F[B] = fa.get.map(f)
          override def set(a: B): F[Unit] = fa.set(g(a))
          override def getAndSet(a: B): F[B] = fa.getAndSet(g(a)).map(f)
          override val access: F[(B, B => F[Boolean])] =
            fa.access.map(_.bimap(f, _.compose(g)))
          override def tryUpdate(f2: B => B): F[Boolean] =
            fa.tryUpdate(g.compose(f2).compose(f))
          override def tryModify[C](f2: B => (B, C)): F[Option[C]] =
            fa.tryModify(f2.compose(f).map(_.leftMap(g)))
          override def update(f2: B => B): F[Unit] =
            fa.update(g.compose(f2).compose(f))
          override def modify[C](f2: B => (B, C)): F[C] =
            fa.modify(f2.compose(f).map(_.leftMap(g)))
          override def tryModifyState[C](state: State[B, C]): F[Option[C]] =
            fa.tryModifyState(state.dimap(f)(g))
          override def modifyState[C](state: State[B, C]): F[C] =
            fa.modifyState(state.dimap(f)(g))
        }
    }
}
