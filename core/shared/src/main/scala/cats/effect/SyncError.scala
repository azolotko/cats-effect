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

import cats.data._
import cats.effect.concurrent.RefError
import cats.syntax.all._

import scala.annotation.implicitNotFound

/**
 * A monad that can suspend the execution of side effects
 * in the `F[_]` context.
 */
@implicitNotFound("Could not find an instance of SyncError for ${F}")
trait SyncError[F[_], E] extends Bracket[F, E] with Defer[F] {

  /**
   * Suspends the evaluation of an `F` reference.
   *
   * Equivalent to `FlatMap.flatten` for pure expressions,
   * the purpose of this function is to suspend side effects
   * in `F`.
   */
  def suspend[A](thunk: => F[A]): F[A]

  /**
   * Alias for `suspend` that suspends the evaluation of
   * an `F` reference and implements `cats.Defer` typeclass.
   */
  final override def defer[A](fa: => F[A]): F[A] = suspend(fa)

  /**
   * Lifts any by-name parameter into the `F` context.
   *
   * Equivalent to `Applicative.pure` for pure expressions,
   * the purpose of this function is to suspend side effects
   * in `F`.
   */
  def delay[A](thunk: => A): F[A] = suspend(pure(thunk))
}

object SyncError {

  /**
   * [[SyncError]] instance built for `cats.data.EitherT` values initialized
   * with any `F` data type that also implements `SyncError`.
   */
  implicit def catsEitherTSyncError[F[_], E](implicit syncError: SyncError[F, E]): SyncError[EitherT[F, E, *], E] =
    new EitherTSyncError[F, E] { def F = SyncError[F, E] }

  /**
   * [[SyncError]] instance built for `cats.data.OptionT` values initialized
   * with any `F` data type that also implements `SyncError`.
   */
  implicit def catsOptionTSyncError[F[_], E](implicit syncError: SyncError[F, E]): SyncError[OptionT[F, *], E] =
    new OptionTSyncError[F, E] { def F = SyncError[F, E] }

  /**
   * [[SyncError]] instance built for `cats.data.StateT` values initialized
   * with any `F` data type that also implements `SyncError`.
   */
  implicit def catsStateTSyncError[F[_], E, S](implicit syncError: SyncError[F, E]): SyncError[StateT[F, S, *], E] =
    new StateTSyncError[F, E, S] { def F = SyncError[F, E] }

  /**
   * [[SyncError]] instance built for `cats.data.WriterT` values initialized
   * with any `F` data type that also implements `SyncError`.
   */
  implicit def catsWriterTSyncError[F[_], E, L: Monoid](
    implicit syncError: SyncError[F, E]
  ): SyncError[WriterT[F, L, *], E] =
    new WriterTSyncError[F, E, L] { def F = SyncError[F, E]; def L = Monoid[L] }

  /**
   * [[SyncError]] instance built for `cats.data.Kleisli` values initialized
   * with any `F` data type that also implements `SyncError`.
   */
  implicit def catsKleisliSyncError[F[_], E, L, R](
    implicit syncError: SyncError[F, E]
  ): SyncError[Kleisli[F, R, *], E] =
    new KleisliSyncError[F, R, E] { def F = SyncError[F, E] }

  /**
   * [[SyncError]] instance built for `cats.data.IorT` values initialized
   * with any `F` data type that also implements `SyncError`.
   */
  implicit def catsIorTSyncError[F[_], E, L: Semigroup](
    implicit syncError: SyncError[F, E]
  ): SyncError[IorT[F, L, *], E] =
    new IorTSyncError[F, E, L] { def F = SyncError[F, E]; def L = Semigroup[L] }

  /**
   * [[SyncError]] instance built for `cats.data.ReaderWriterStateT` values initialized
   * with any `F` data type that also implements `SyncError`.
   */
  implicit def catsReaderWriteStateTSyncError[F[_], E, Env, L: Monoid, S](
    implicit syncError: SyncError[F, E]
  ): SyncError[ReaderWriterStateT[F, Env, L, S, *], E] =
    new ReaderWriterStateTSyncError[F, E, Env, L, S] { def F = SyncError[F, E]; def L = Monoid[L] }

  private[effect] trait EitherTSyncError[F[_], L] extends SyncError[EitherT[F, L, *], L] {
    implicit protected def F: SyncError[F, L]

    def pure[A](x: A): EitherT[F, L, A] =
      EitherT.pure(x)

    def handleErrorWith[A](fa: EitherT[F, L, A])(f: L => EitherT[F, L, A]): EitherT[F, L, A] =
      EitherT(F.handleErrorWith(fa.value)(f.andThen(_.value)))

    def raiseError[A](e: L): EitherT[F, L, A] =
      EitherT.liftF(F.raiseError(e))

    def bracketCase[A, B](
      acquire: EitherT[F, L, A]
    )(use: A => EitherT[F, L, B])(release: (A, ExitCase[L]) => EitherT[F, L, Unit]): EitherT[F, L, B] =
      EitherT.liftF(RefError.of[F, L, Option[L]](None)).flatMap { ref =>
        EitherT(
          F.bracketCase(acquire.value) {
              case Right(a)    => use(a).value
              case l @ Left(_) => F.pure(l.rightCast[B])
            } {
              case (Left(_), _) => F.unit //Nothing to release
              case (Right(a), ExitCase.Completed) =>
                release(a, ExitCase.Completed).value.flatMap {
                  case Left(l)  => ref.set(Some(l))
                  case Right(_) => F.unit
                }
              case (Right(a), res) => release(a, res).value.void
            }
            .flatMap[Either[L, B]] {
              case r @ Right(_) => ref.get.map(_.fold(r: Either[L, B])(Either.left[L, B]))
              case l @ Left(_)  => F.pure(l)
            }
        )
      }

    def flatMap[A, B](fa: EitherT[F, L, A])(f: A => EitherT[F, L, B]): EitherT[F, L, B] =
      fa.flatMap(f)

    def tailRecM[A, B](a: A)(f: A => EitherT[F, L, Either[A, B]]): EitherT[F, L, B] =
      EitherT.catsDataMonadErrorForEitherT[F, L].tailRecM(a)(f)

    def suspend[A](thunk: => EitherT[F, L, A]): EitherT[F, L, A] =
      EitherT(F.suspend(thunk.value))

    override def uncancelable[A](fa: EitherT[F, L, A]): EitherT[F, L, A] =
      EitherT(F.uncancelable(fa.value))
  }

  private[effect] trait OptionTSyncError[F[_], E] extends SyncError[OptionT[F, *], E] {
    implicit protected def F: SyncError[F, E]

    def pure[A](x: A): OptionT[F, A] = OptionT.pure(x)

    def handleErrorWith[A](fa: OptionT[F, A])(f: E => OptionT[F, A]): OptionT[F, A] =
      OptionT.catsDataMonadErrorForOptionT[F, E].handleErrorWith(fa)(f)

    def raiseError[A](e: E): OptionT[F, A] =
      OptionT.catsDataMonadErrorForOptionT[F, E].raiseError(e)

    def bracketCase[A, B](
      acquire: OptionT[F, A]
    )(use: A => OptionT[F, B])(release: (A, ExitCase[E]) => OptionT[F, Unit]): OptionT[F, B] =
      //Boolean represents if release returned None
      OptionT.liftF(RefError.of[F, E, Boolean](false)).flatMap { ref =>
        OptionT(
          F.bracketCase(acquire.value) {
              case Some(a) => use(a).value
              case None    => F.pure(Option.empty[B])
            } {
              case (None, _) => F.unit //Nothing to release
              case (Some(a), ExitCase.Completed) =>
                release(a, ExitCase.Completed).value.flatMap {
                  case None    => ref.set(true)
                  case Some(_) => F.unit
                }
              case (Some(a), res) => release(a, res).value.void
            }
            .flatMap[Option[B]] {
              case s @ Some(_) => ref.get.map(b => if (b) None else s)
              case None        => F.pure(None)
            }
        )
      }

    def flatMap[A, B](fa: OptionT[F, A])(f: A => OptionT[F, B]): OptionT[F, B] =
      fa.flatMap(f)

    def tailRecM[A, B](a: A)(f: A => OptionT[F, Either[A, B]]): OptionT[F, B] =
      OptionT.catsDataMonadErrorForOptionT[F, E].tailRecM(a)(f)

    def suspend[A](thunk: => OptionT[F, A]): OptionT[F, A] =
      OptionT(F.suspend(thunk.value))

    override def uncancelable[A](fa: OptionT[F, A]): OptionT[F, A] =
      OptionT(F.uncancelable(fa.value))
  }

  private[effect] trait StateTSyncError[F[_], E, S] extends SyncError[StateT[F, S, *], E] {
    implicit protected def F: SyncError[F, E]

    def pure[A](x: A): StateT[F, S, A] = StateT.pure(x)

    def handleErrorWith[A](fa: StateT[F, S, A])(f: E => StateT[F, S, A]): StateT[F, S, A] =
      StateT(s => F.handleErrorWith(fa.run(s))(e => f(e).run(s)))

    def raiseError[A](e: E): StateT[F, S, A] =
      StateT.liftF(F.raiseError(e))

    def bracketCase[A, B](
      acquire: StateT[F, S, A]
    )(use: A => StateT[F, S, B])(release: (A, ExitCase[E]) => StateT[F, S, Unit]): StateT[F, S, B] =
      StateT.liftF(RefError.of[F, E, Option[S]](None)).flatMap { ref =>
        StateT { startS =>
          F.bracketCase[(S, A), (S, B)](acquire.run(startS)) {
              case (s, a) =>
                use(a).run(s).flatTap { case (s, _) => ref.set(Some(s)) }
            } {
              case ((oldS, a), ExitCase.Completed) =>
                ref.get
                  .map(_.getOrElse(oldS))
                  .flatMap(s => release(a, ExitCase.Completed).runS(s))
                  .flatMap(s => ref.set(Some(s)))
              case ((s, a), br) =>
                release(a, br).run(s).void
            }
            .flatMap { case (s, b) => ref.get.map(_.getOrElse(s)).tupleRight(b) }
        }
      }

    override def uncancelable[A](fa: StateT[F, S, A]): StateT[F, S, A] =
      fa.transformF(F.uncancelable)

    def flatMap[A, B](fa: StateT[F, S, A])(f: A => StateT[F, S, B]): StateT[F, S, B] =
      fa.flatMap(f)

    // overwriting the pre-existing one, since flatMap is guaranteed stack-safe
    def tailRecM[A, B](a: A)(f: A => StateT[F, S, Either[A, B]]): StateT[F, S, B] =
      IndexedStateT.catsDataMonadForIndexedStateT[F, S].tailRecM(a)(f)

    def suspend[A](thunk: => StateT[F, S, A]): StateT[F, S, A] =
      StateT.applyF(F.suspend(thunk.runF))
  }

  private[effect] trait WriterTSyncError[F[_], E, L] extends SyncError[WriterT[F, L, *], E] {
    implicit protected def F: SyncError[F, E]
    implicit protected def L: Monoid[L]

    def pure[A](x: A): WriterT[F, L, A] = WriterT.value(x)

    def handleErrorWith[A](fa: WriterT[F, L, A])(f: E => WriterT[F, L, A]): WriterT[F, L, A] =
      WriterT.catsDataMonadErrorForWriterT[F, L, E].handleErrorWith(fa)(f)

    def raiseError[A](e: E): WriterT[F, L, A] =
      WriterT.catsDataMonadErrorForWriterT[F, L, E].raiseError(e)

    def bracketCase[A, B](
      acquire: WriterT[F, L, A]
    )(use: A => WriterT[F, L, B])(release: (A, ExitCase[E]) => WriterT[F, L, Unit]): WriterT[F, L, B] =
      WriterT(
        RefError[F, E].of(L.empty).flatMap { ref =>
          F.bracketCase(acquire.run) { la =>
              WriterT(la.pure[F]).flatMap(use).run
            } {
              case ((_, a), ec) =>
                val r = release(a, ec).run
                if (ec == ExitCase.Completed)
                  r.flatMap { case (l, _) => ref.set(l) }
                else
                  r.void
            }
            .flatMap { lb =>
              ref.get.map(l => lb.leftMap(_ |+| l))
            }
        }
      )

    override def uncancelable[A](fa: WriterT[F, L, A]): WriterT[F, L, A] =
      WriterT(F.uncancelable(fa.run))

    def flatMap[A, B](fa: WriterT[F, L, A])(f: A => WriterT[F, L, B]): WriterT[F, L, B] =
      fa.flatMap(f)

    def tailRecM[A, B](a: A)(f: A => WriterT[F, L, Either[A, B]]): WriterT[F, L, B] =
      WriterT.catsDataMonadForWriterT[F, L].tailRecM(a)(f)

    def suspend[A](thunk: => WriterT[F, L, A]): WriterT[F, L, A] =
      WriterT(F.suspend(thunk.run))
  }

  abstract private[effect] class KleisliSyncError[F[_], R, E]
      extends Bracket.KleisliBracket[F, R, E]
      with SyncError[Kleisli[F, R, *], E] {
    implicit override protected def F: SyncError[F, E]

    override def handleErrorWith[A](fa: Kleisli[F, R, A])(f: E => Kleisli[F, R, A]): Kleisli[F, R, A] =
      Kleisli { r =>
        F.suspend(F.handleErrorWith(fa.run(r))(e => f(e).run(r)))
      }

    override def flatMap[A, B](fa: Kleisli[F, R, A])(f: A => Kleisli[F, R, B]): Kleisli[F, R, B] =
      Kleisli { r =>
        F.suspend(fa.run(r).flatMap(f.andThen(_.run(r))))
      }

    def suspend[A](thunk: => Kleisli[F, R, A]): Kleisli[F, R, A] =
      Kleisli(r => F.suspend(thunk.run(r)))

    override def uncancelable[A](fa: Kleisli[F, R, A]): Kleisli[F, R, A] =
      Kleisli { r =>
        F.suspend(F.uncancelable(fa.run(r)))
      }
  }

  private[effect] trait IorTSyncError[F[_], E, L] extends SyncError[IorT[F, L, *], E] {
    implicit protected def F: SyncError[F, E]
    implicit protected def L: Semigroup[L]

    def pure[A](x: A): IorT[F, L, A] =
      IorT.pure(x)

    def handleErrorWith[A](fa: IorT[F, L, A])(f: E => IorT[F, L, A]): IorT[F, L, A] =
      IorT(F.handleErrorWith(fa.value)(f.andThen(_.value)))

    def raiseError[A](e: E): IorT[F, L, A] =
      IorT.liftF(F.raiseError(e))

    def bracketCase[A, B](
      acquire: IorT[F, L, A]
    )(use: A => IorT[F, L, B])(release: (A, ExitCase[E]) => IorT[F, L, Unit]): IorT[F, L, B] =
      IorT.liftF(RefError[F, E].of(().rightIor[L])).flatMapF { ref =>
        F.bracketCase(acquire.value) { ia =>
            IorT.fromIor[F](ia).flatMap(use).value
          } { (ia, ec) =>
            ia.toOption.fold(F.unit) { a =>
              val r = release(a, ec).value
              if (ec == ExitCase.Completed)
                r.flatMap {
                  case Ior.Right(_) => F.unit
                  case other        => ref.set(other.void)
                }
              else
                r.void
            }
          }
          .flatMap {
            case l @ Ior.Left(_) => F.pure(l)
            case other           => ref.get.map(other <* _)
          }
      }

    def flatMap[A, B](fa: IorT[F, L, A])(f: A => IorT[F, L, B]): IorT[F, L, B] =
      fa.flatMap(f)

    def tailRecM[A, B](a: A)(f: A => IorT[F, L, Either[A, B]]): IorT[F, L, B] =
      IorT.catsDataMonadErrorForIorT[F, L].tailRecM(a)(f)

    def suspend[A](thunk: => IorT[F, L, A]): IorT[F, L, A] =
      IorT(F.suspend(thunk.value))

    override def uncancelable[A](fa: IorT[F, L, A]): IorT[F, L, A] =
      IorT(F.uncancelable(fa.value))
  }

  private[effect] trait ReaderWriterStateTSyncError[F[_], E, Env, L, S]
      extends SyncError[ReaderWriterStateT[F, Env, L, S, *], E] {
    implicit protected def F: SyncError[F, E]
    implicit protected def L: Monoid[L]

    def pure[A](x: A): ReaderWriterStateT[F, Env, L, S, A] =
      ReaderWriterStateT.pure(x)

    def handleErrorWith[A](
      fa: ReaderWriterStateT[F, Env, L, S, A]
    )(f: E => ReaderWriterStateT[F, Env, L, S, A]): ReaderWriterStateT[F, Env, L, S, A] =
      ReaderWriterStateT((e, s) => F.handleErrorWith(fa.run(e, s))(f.andThen(_.run(e, s))))

    def raiseError[A](e: E): ReaderWriterStateT[F, Env, L, S, A] =
      ReaderWriterStateT.liftF(F.raiseError(e))

    def bracketCase[A, B](acquire: ReaderWriterStateT[F, Env, L, S, A])(
      use: A => ReaderWriterStateT[F, Env, L, S, B]
    )(
      release: (A, ExitCase[E]) => ReaderWriterStateT[F, Env, L, S, Unit]
    ): ReaderWriterStateT[F, Env, L, S, B] =
      ReaderWriterStateT.liftF(RefError[F, E].of[(L, Option[S])]((L.empty, None))).flatMap { ref =>
        ReaderWriterStateT { (e, startS) =>
          F.bracketCase(acquire.run(e, startS)) {
              case (l, s, a) =>
                ReaderWriterStateT.pure[F, Env, L, S, A](a).tell(l).flatMap(use).run(e, s).flatTap {
                  case (l, s, _) => ref.set((l, Some(s)))
                }
            } {
              case ((_, oldS, a), ExitCase.Completed) =>
                ref.get
                  .map(_._2.getOrElse(oldS))
                  .flatMap(s => release(a, ExitCase.Completed).run(e, s))
                  .flatMap { case (l, s, _) => ref.set((l, Some(s))) }
              case ((_, s, a), ec) =>
                release(a, ec).run(e, s).void
            }
            .flatMap {
              case (l, s, b) =>
                ref.get.map { case (l2, s2) => (l |+| l2, s2.getOrElse(s), b) }
            }
        }
      }

    def flatMap[A, B](
      fa: ReaderWriterStateT[F, Env, L, S, A]
    )(f: A => ReaderWriterStateT[F, Env, L, S, B]): ReaderWriterStateT[F, Env, L, S, B] =
      fa.flatMap(f)

    def tailRecM[A, B](
      a: A
    )(f: A => ReaderWriterStateT[F, Env, L, S, Either[A, B]]): ReaderWriterStateT[F, Env, L, S, B] =
      IndexedReaderWriterStateT.catsDataMonadForRWST[F, Env, L, S].tailRecM(a)(f)

    def suspend[A](thunk: => ReaderWriterStateT[F, Env, L, S, A]): ReaderWriterStateT[F, Env, L, S, A] =
      ReaderWriterStateT((e, s) => F.suspend(thunk.run(e, s)))

    override def uncancelable[A](fa: ReaderWriterStateT[F, Env, L, S, A]): ReaderWriterStateT[F, Env, L, S, A] =
      ReaderWriterStateT((e, s) => F.uncancelable(fa.run(e, s)))
  }

  /**
   * Summon an instance of [[SyncError]] for `F`.
   */
  @inline def apply[F[_], E](implicit instance: SyncError[F, E]): SyncError[F, E] = instance

  trait Ops[F[_], E, A] {
    type TypeClassType <: SyncError[F, E]
    def self: F[A]
    val typeClassInstance: TypeClassType
  }
  trait AllOps[F[_], E, A] extends Ops[F, E, A] {
    type TypeClassType <: SyncError[F, E]
  }
  trait ToSyncOps {
    implicit def toSyncOps[F[_], E, A](target: F[A])(implicit tc: SyncError[F, E]): Ops[F, E, A] {
      type TypeClassType = SyncError[F, E]
    } = new Ops[F, E, A] {
      type TypeClassType = SyncError[F, E]
      val self: F[A] = target
      val typeClassInstance: TypeClassType = tc
    }
  }
  object nonInheritedOps extends ToSyncOps

  // indirection required to avoid spurious static forwarders that conflict on case-insensitive filesystems (scala-js/scala-js#4148)
  class ops$ {
    implicit def toAllSyncOps[F[_], E, A](target: F[A])(implicit tc: SyncError[F, E]): AllOps[F, E, A] {
      type TypeClassType = SyncError[F, E]
    } = new AllOps[F, E, A] {
      type TypeClassType = SyncError[F, E]
      val self: F[A] = target
      val typeClassInstance: TypeClassType = tc
    }
  }
  // TODO this lacks a MODULE$ field; is that okay???
  val ops = new ops$
}
