import openturns as ot
import pickle
from logger import logger


def create_metamodel(train_coordinates, train_values, path='.', name_postfix='kmodel', save=False):
    # this module is responsible to create a kriging metamodel from the inputs and save
    # the metamodel in pickle format

    # turn the warnings off
    ot.Log.Show(ot.Log.NONE)

    if len(train_coordinates) != len(train_values):
        logger.info(
            "train coordinates and train values must have the same size.")
        return None

    try:
        train_coordinates = ot.Sample(train_coordinates)
        train_values = ot.Sample(train_values)

        # Fit
        inputDimension = 2
        basis = ot.ConstantBasisFactory(inputDimension).build()
        covarianceModel = ot.SquaredExponential([1.] * inputDimension, [1.0])
        algo = ot.KrigingAlgorithm(
            train_coordinates, train_values, covarianceModel, basis)
        algo.run()
        result = algo.getResult()
        krigingMetamodel = result.getMetaModel()

        # save kriging metamodel
        if save:
            model_name = f'{path}/metamodel_{name_postfix}.pkl'
            pickle.dump(krigingMetamodel, open(model_name, 'wb'))

        return krigingMetamodel

    except Exception as ex:
        logger.info(f"An exception is occurred. Error: {ex}")
        return None


def load_metamodel(fullpath):
    # this module is responsible to load a metamodel from the specified path

    try:
        return (pickle.load(open(fullpath, 'rb')))

    except Exception as ex:
        logger.info(f"An exception is occurred. Error: {ex}")
        return None
