import strategy.simple_single_signal_k_pl_bs.auto_generator_by_db as generator
g = generator.Generator()
g.build_debug()
q = g.get_training_random_single(size=100000)
print(q)

