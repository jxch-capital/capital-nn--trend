import tensorflow as tf
import matplotlib.pyplot as plt
import numpy as np
import indicators.advantage_excursion as ae

training_nor_k = np.load('training_nor_k.npz')
validation_nor_k = np.load('validation_nor_k.npz')
training_y = np.load('training_y.npz')
validation_y = np.load('validation_y.npz')

training_mark2 = ae.re_code_total_mark2(training_y['label_total_hot'])
validation_mark2 = ae.re_code_total_mark2(validation_y['label_total_hot'])

model = tf.keras.models.Sequential([
    tf.keras.layers.LSTM(128, input_shape=(120, 4)),
    tf.keras.layers.Dense(1, activation='sigmoid'),
])
model.build()
model.summary()

model.compile(loss='binary_crossentropy', optimizer=tf.keras.optimizers.Adam(learning_rate=0.01), metrics=['acc'])
his = model.fit(training_nor_k['k'], training_mark2, epochs=20, validation_data=(validation_nor_k['k'], validation_mark2))


